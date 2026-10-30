package integration;

import database.ConnectionPool;
import org.junit.jupiter.api.Test;
import support.CheckoutDeployment;
import support.FaultInjection;
import support.InventoryTestSchema;
import support.ServiceHttp;
import support.ServiceProcesses;
import support.TestSchema;
import support.TransactionTestSchema;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Background-completion accounting (FR-061, FR-066, R9; 006 contracts/checkout-saga.md): a checkout whose
 * client got 500 and that its outcome event completed later counts as one background completion, and one
 * completed by the redelivered outcome after Transaction halted before answering counts as a completion
 * without a recorded response. {@code db/validate-invariants.ps1} prints both counts. The deployment uses the
 * baseline catalog size and stock, so the script's cross-service reconciliation applies unchanged. Under races
 * between the outcome handler's claim and the request's timeout claim, exactly one of them writes
 * {@code client_outcome} (D7).
 */
class BackgroundCompletionAccountingTest {
    private static final int PRODUCTS = 2000;
    private static final int STOCK = 10_000;
    private static final Duration SETTLE = Duration.ofSeconds(60);

    @Test
    void aLateCompletionAndAnUnansweredCompletionAreCountedApart() throws Exception {
        try (CheckoutDeployment deployment = CheckoutDeployment.start(PRODUCTS, STOCK)) {
            ServiceHttp http = deployment.transactionHttp;
            String a = InventoryTestSchema.sku(1);
            String b = InventoryTestSchema.sku(2);

            // Inventory is delayed past the 8 s budget: its stock row is locked, so its apply waits.
            String late = http.start("late-station");
            assertEquals(200, http.scan(late, a).statusCode());
            ConnectionPool lockPool = deployment.inventorySchema.newPool("test.stock-lock", 1, 1, null);
            Connection blocker = lockPool.borrow();
            try {
                try (PreparedStatement lock = blocker.prepareStatement(
                        "SELECT stock_quantity FROM inventory WHERE sku = ? FOR UPDATE")) {
                    lock.setString(1, a);
                    try (ResultSet ignored = lock.executeQuery()) {
                        HttpResponse<String> pending = http.complete(late);
                        assertEquals(500, pending.statusCode(), pending.body() + deployment.logs());
                        assertEquals("INTERNAL_ERROR", ServiceHttp.json(pending).get("error"));
                        TransactionTestSchema.AttemptRow attempt = deployment.transactionSchema.attempts(late).get(0);
                        assertEquals("PENDING", attempt.state());
                        assertEquals("PENDING_ERROR", attempt.clientOutcome(),
                                "PENDING_ERROR was committed before the 500 was sent");
                    }
                }
            } finally {
                blocker.rollback();
                lockPool.release(blocker);
            }
            awaitSettled(deployment, late);
            TransactionTestSchema.AttemptRow background = deployment.transactionSchema.attempts(late).get(0);
            assertEquals("COMPLETED", background.state(), "its outcome completed it after the client's 500");
            assertEquals("PENDING_ERROR", background.clientOutcome(), "one background completion");
            assertEquals(STOCK - 1, deployment.inventorySchema.stock(a));

            // Transaction halts on consuming Inventory's outcome, before T3 and before answering; the outcome
            // is redelivered to the restarted process, which has no responder for it.
            deployment.startTransaction(FaultInjection.haltAt("checkout.outcome.beforeCommit"));
            String unanswered = http.start("halt-station");
            assertEquals(200, http.scan(unanswered, b).statusCode());
            assertThrows(IOException.class, () -> http.complete(unanswered), "the halted process never answers");
            assertEquals(137, deployment.transactionProcess().awaitExit(Duration.ofSeconds(30)));
            deployment.startTransaction();
            awaitSettled(deployment, unanswered);
            TransactionTestSchema.AttemptRow silent = deployment.transactionSchema.attempts(unanswered).get(0);
            assertEquals("COMPLETED", silent.state());
            assertEquals("NONE", silent.clientOutcome(), "a completion without a recorded response");

            assertEquals(1, deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM checkout_attempt "
                    + "WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'"));
            assertEquals(1, deployment.transactionSchema.queryLong("SELECT COUNT(*) FROM checkout_attempt "
                    + "WHERE state = 'COMPLETED' AND client_outcome = 'NONE'"));

            String report = validateInvariants(deployment);
            assertTrue(report.contains("completed_transactions=2"), report);
            assertTrue(report.contains("background_completions=1"), report);
            assertTrue(report.contains("completions_without_response=1"), report);
            assertTrue(report.contains("PASS"), report);
        }
    }

    @Test
    void underRacesBetweenTheHandlersClaimAndTheTimeoutClaimExactlyOneWritesTheClientOutcome() throws Exception {
        try (support.TransactionTestSchema schema = support.TransactionTestSchema.create()) {
            schema.seedReplica(5, java.util.UUID.randomUUID().toString());
            Duration budget = Duration.ofMillis(200);
            support.InProcessTransaction service = support.InProcessTransaction.over(schema).pool(4, 16)
                    .settings(transaction.CheckoutOrchestrator.Settings.defaults().withResponseBudget(budget)).build();
            java.util.Random random = new java.util.Random(87);
            try (support.FakeInventoryReactor inventory = service.reactor().otherwise(call ->
                    support.FakeInventoryReactor.Reply.deduct().delayed(Duration.ofMillis(150 + random.nextInt(100))))
                    .start()) {
                java.util.Map<String, String> answers = new java.util.concurrent.ConcurrentHashMap<>();
                try (var stations = java.util.concurrent.Executors.newFixedThreadPool(8)) {
                    java.util.List<java.util.concurrent.Future<?>> work = new java.util.ArrayList<>();
                    for (int i = 0; i < 60; i++) {
                        String id = service.start();
                        service.scan(id, InventoryTestSchema.sku(1 + i % 5));
                        work.add(stations.submit(() -> {
                            try {
                                service.complete(id);
                                answers.put(id, "200");
                            } catch (transaction.CheckoutPendingFailure pending) {
                                answers.put(id, "500");
                            }
                        }));
                    }
                    for (var done : work) done.get(60, TimeUnit.SECONDS);
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (schema.pendingAttemptCount() > 0) {
                    assertTrue(System.nanoTime() < deadline, "every delayed outcome settles its attempt");
                    Thread.sleep(50);
                }
                int success = 0;
                int background = 0;
                for (var answer : answers.entrySet()) {
                    TransactionTestSchema.AttemptRow attempt = schema.attempts(answer.getKey()).get(0);
                    assertEquals("COMPLETED", attempt.state());
                    String expected = answer.getValue().equals("200") ? "SUCCESS" : "PENDING_ERROR";
                    assertEquals(expected, attempt.clientOutcome(),
                            answer.getKey() + ": the outcome recorded is the one the client got");
                    if (expected.equals("SUCCESS")) success++; else background++;
                }
                assertEquals(60, success + background);
                assertEquals(background, schema.queryLong("SELECT COUNT(*) FROM checkout_attempt "
                        + "WHERE state = 'COMPLETED' AND client_outcome = 'PENDING_ERROR'"), "background completions, exactly");
                assertEquals(0, schema.queryLong("SELECT COUNT(*) FROM checkout_attempt "
                        + "WHERE state = 'COMPLETED' AND client_outcome = 'NONE'"), "every answered completion has its outcome");
            }
        }
    }

    /** Runs {@code db/validate-invariants.ps1 -SkipAnalytics} against the disposable schemas and their users. */
    private static String validateInvariants(CheckoutDeployment deployment) throws Exception {
        assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows"),
                "db/validate-invariants.ps1 runs under Windows PowerShell");
        Path script = ServiceProcesses.serverRoot().getParent().resolve("db/validate-invariants.ps1");
        assertTrue(Files.isRegularFile(script), script.toString());
        TestSchema transaction = deployment.transactionSchema;
        TestSchema inventory = deployment.inventorySchema;
        ProcessBuilder builder = new ProcessBuilder(List.of("powershell", "-NoProfile", "-NonInteractive",
                "-ExecutionPolicy", "Bypass", "-File", script.toString(), "-SkipAnalytics",
                "-MySqlHost", transaction.host(), "-Port", Integer.toString(transaction.port())));
        String mysql = System.getProperty("MYSQL_EXE", System.getenv("MYSQL_EXE"));
        if (mysql != null && !mysql.isBlank()) {
            builder.command().add("-MySqlExe");
            builder.command().add(mysql);
        }
        Map<String, String> environment = builder.environment();
        environment.put("TRANSACTION_DB_NAME", transaction.name());
        environment.put("TRANSACTION_DB_USER", transaction.user());
        environment.put("TRANSACTION_DB_PASSWORD", transaction.password());
        environment.put("INVENTORY_DB_NAME", inventory.name());
        environment.put("INVENTORY_DB_USER", inventory.user());
        environment.put("INVENTORY_DB_PASSWORD", inventory.password());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(120, TimeUnit.SECONDS), "validate-invariants.ps1 did not finish");
        assertEquals(0, process.exitValue(), "validate-invariants.ps1 failed:" + System.lineSeparator() + output);
        return output;
    }

    private static void awaitSettled(CheckoutDeployment deployment, String id) throws Exception {
        long deadline = System.nanoTime() + SETTLE.toNanos();
        while (true) {
            List<TransactionTestSchema.AttemptRow> attempts = deployment.transactionSchema.attempts(id);
            if (!attempts.isEmpty() && attempts.stream().noneMatch(attempt -> attempt.state().equals("PENDING"))) return;
            if (System.nanoTime() > deadline) fail(id + ": the attempt never settled " + attempts + deployment.logs());
            Thread.sleep(100);
        }
    }
}
