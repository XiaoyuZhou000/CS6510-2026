import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quickstart scenario E driver (007 T091, research N12, contracts/subscriber-client-and-reconciliation.md
 * "ChangeScenarioMain"): subscription changes at controlled points during sales, recorded in {@code changes.jsonl} for
 * {@code ReconcileMain --mode=history}.
 *
 * <pre>
 * ChangeScenarioMain --out=DIR [--subscribers=10] [--seed=N] [--base-url=http://localhost:8080]
 *                    [--bootstrap=127.0.0.1:19092] [--with-load] [--faults-endpoint=URL]
 * </pre>
 *
 * Steps, in order:
 * <ol>
 *   <li>Register {@code chg-0001..N}, each with three SKUs of a seeded pool of 12, and consume each channel from its
 *       first offset into {@code DIR/receipts/} (as {@code SubscriberMain} does).</li>
 *   <li>Confirmed-before cases: register, add, remove, cancel, and re-register. After each confirmation it completes a
 *       sale of an affected SKU through {@code POST /transactions}, {@code /items}, and {@code /complete}, recording the
 *       highest version confirmed before the Complete was sent. Just before the cancel it also sells an SKU the
 *       subscriber still holds and waits until the sale is selected (with its channel stalled through
 *       {@code --faults-endpoint}), so a notification selected before the cancellation is delivered after it.</li>
 *   <li>A burst of 50 seeded random changes while the driver's own sales run. With {@code --with-load} it first waits
 *       for {@code DIR/load.started} (an external Load Client has started) and afterwards for {@code DIR/load.done}.</li>
 *   <li>With {@code --faults-endpoint}: three changes failed by {@code notification.subscription.beforeCommit:throw}
 *       (expected 500), then a sale of the guarded SKUs.</li>
 *   <li>One request of each kind FR-006 rejects (expected 400, 404, 409).</li>
 *   <li>The drain: Notification has nothing pending and every channel is read to its end, twice in a row.</li>
 * </ol>
 * Failed and rejected requests target guarded subscribers: {@code changes.jsonl} lists, for each, the history versions
 * at which it may change, so the reconciler can check they appear nowhere else. Exit codes: 0 every request answered as
 * expected and the run drained; 1 an unexpected answer or no drain; 2 usage; 3 setup failure.
 */
public final class ChangeScenarioMain {
    static final String PREFIX = "chg-";
    static final int POOL = 12;
    static final int SKUS_PER_SUBSCRIBER = 3;
    static final int BURST = 50;
    static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(120);
    static final String FAULT = "notification.subscription.beforeCommit:throw";

    private final String baseUrl;
    private final String bootstrap;
    private final Path out;
    private final boolean withLoad;
    private final String faultsEndpoint;
    private final int subscribers;
    private final SplittableRandom random;
    private final PrintStream log;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final BufferedWriter changes;
    private final AtomicLong confirmed = new AtomicLong();
    private final Map<String, SubscriberThread> consumers = new ConcurrentHashMap<>();
    private final List<Thread> runners = new ArrayList<>();
    private final List<String> unexpected = new ArrayList<>();
    private List<String> pool;

    /** One HTTP answer. */
    record Answer(int status, Map<String, Object> body) {
        String error() { return body == null ? null : (String) body.get("error"); }

        Long version() {
            return body != null && body.get("subscriptionVersion") instanceof Long version ? version : null;
        }
    }

    private ChangeScenarioMain(Map<String, String> options, PrintStream log) throws IOException {
        this.baseUrl = options.getOrDefault("base-url", "http://localhost:8080").replaceAll("/+$", "");
        this.bootstrap = options.getOrDefault("bootstrap", "127.0.0.1:19092");
        this.out = Path.of(options.get("out")).toAbsolutePath().normalize();
        this.withLoad = options.containsKey("with-load");
        this.faultsEndpoint = options.get("faults-endpoint");
        this.subscribers = Integer.parseInt(options.getOrDefault("subscribers", "10"));
        if (subscribers < 3 || subscribers > 999) throw new IllegalArgumentException("--subscribers must be 3..999");
        long seed = options.containsKey("seed") ? Long.parseLong(options.get("seed")) : new SecureRandom().nextLong();
        this.random = new SplittableRandom(seed);
        this.log = log;
        Files.createDirectories(out.resolve("receipts"));
        if (Files.exists(out.resolve("changes.jsonl"))) throw new IOException("changes.jsonl already exists in " + out);
        this.changes = Files.newBufferedWriter(out.resolve("changes.jsonl"), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        write(Json.object("kind", "run", "seed", seed, "subscribers", (long) subscribers, "withLoad", withLoad,
                "faults", faultsEndpoint != null, "baseUrl", baseUrl, "startedAtMs", System.currentTimeMillis()));
        log.println("change scenario: seed " + seed + ", " + subscribers + " subscribers, out " + out);
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    static int run(String[] args, PrintStream log) {
        Map<String, String> options = new LinkedHashMap<>();
        for (String arg : args) {
            if (arg.equals("--with-load")) options.put("with-load", "true");
            else if (arg.startsWith("--") && arg.contains("=")) {
                String name = arg.substring(2, arg.indexOf('='));
                if (!List.of("out", "subscribers", "seed", "base-url", "bootstrap", "faults-endpoint").contains(name)) {
                    return usage("Unknown option: " + arg);
                }
                options.put(name, arg.substring(arg.indexOf('=') + 1));
            } else return usage("Unknown argument: " + arg);
        }
        if (!options.containsKey("out")) return usage("--out=DIR is required");
        ChangeScenarioMain scenario;
        try {
            scenario = new ChangeScenarioMain(options, log);
        } catch (IllegalArgumentException e) {
            return usage(e.getMessage());
        } catch (IOException e) {
            System.err.println("ChangeScenarioMain: " + e.getMessage());
            return 3;
        }
        try {
            return scenario.execute();
        } catch (Exception e) {
            System.err.println("ChangeScenarioMain failed: " + e);
            e.printStackTrace();
            return 3;
        } finally {
            scenario.close();
        }
    }

    private int execute() throws Exception {
        pool = pickPool(new Registrar(client, baseUrl, log).fetchCatalog());
        write(Json.object("kind", "pool", "skus", new ArrayList<Object>(pool)));

        // 1. The subscriber set.
        for (int i = 1; i <= subscribers; i++) {
            String id = subscriberId(i);
            expect("setup", register(id, randomSkus(SKUS_PER_SUBSCRIBER), false), 201);
        }

        // 2. Confirmed-before cases.
        String newcomer = PREFIX + "new";
        String x = pool.get(0);
        expect("register", register(newcomer, List.of(x), "register", false), 201);
        sale("register", x);
        String first = subscriberId(1);
        String y = notHeldBy(first);
        expect("add", patch("add", first, List.of(y), List.of(), false), 200);
        sale("add", y);
        expect("remove", patch("remove", first, List.of(), List.of(y), false), 200);
        sale("remove", y);
        String second = subscriberId(2);
        List<String> former = skusOf(second);
        // Selected while chg-0002 still holds it, cancelled before it is delivered: the notification is still owed (rule
        // 5). Complete returns before Notification selects the sale, so the driver waits for the selection; with the
        // faults endpoint it also stalls chg-0002's channel, so the delivery comes only after the cancellation.
        long selectedBefore = salesSelected();
        if (faultsEndpoint != null) setFaults("notification.channel.stall:" + second);
        try {
            sale("selected-before-cancel", former.get(0));
            awaitSalesSelected(selectedBefore + 1);
            expect("cancel", delete("cancel", second, false), 200);
        } finally {
            if (faultsEndpoint != null) setFaults("");
        }
        sale("cancel", former.get(0));
        String z = pool.get(pool.size() - 1);
        expect("reregister", register(second, List.of(z), "reregister", false), 201);
        sale("reregister", z);

        // 3. The burst, with the driver's own sales running.
        if (withLoad) awaitFile("load.started", Duration.ofMinutes(5));
        AtomicBoolean selling = new AtomicBoolean(true);
        SplittableRandom sales = random.split();   // split here: SplittableRandom is not thread-safe
        Thread seller = Thread.ofPlatform().name("change-scenario-sales").start(() -> {
            while (selling.get()) {
                try {
                    sale("burst", pool.get(sales.nextInt(pool.size())), pool.get(sales.nextInt(pool.size())));
                } catch (Exception e) {
                    log.println("burst sale failed: " + e);
                }
            }
        });
        try {
            for (int i = 0; i < BURST; i++) {
                randomChange();
                Thread.sleep(40);
            }
        } finally {
            selling.set(false);
            seller.join();
        }
        if (withLoad) awaitFile("load.done", Duration.ofMinutes(10));

        // 4. Failed changes, on guarded subscribers.
        String guard = PREFIX + "guard";
        String s = pool.get(1);
        String t = pool.get(2);
        Answer guarded = register(guard, List.of(s), "guard", false);
        expect("guard", guarded, 201);
        List<Object> guardVersions = new ArrayList<>();
        if (guarded.version() != null) guardVersions.add(guarded.version());
        if (faultsEndpoint != null) {
            setFaults(FAULT);
            try {
                expect("failed", register(PREFIX + "fail-1", List.of(t), "failed", true), 500);
                expect("failed", patch("failed", guard, List.of(t), List.of(), true), 500);
                expect("failed", delete("failed", guard, true), 500);
            } finally {
                setFaults("");
            }
            write(Json.object("kind", "guard", "subscriberId", PREFIX + "fail-1", "allowedVersions", new ArrayList<>(),
                    "reason", "failed"));
            sale("failed", s, t);
        } else {
            log.println("no --faults-endpoint: the failed-change step is skipped");
        }

        // 5. Requests FR-006 rejects; none may change anything.
        String rejected = PREFIX + "rej-1";
        expect("rejected", register(rejected, List.of(), "rejected", true), 400);
        expect("rejected", register(rejected, List.of("SKU-UNKNOWN-1"), "rejected", true), 400);
        expect("rejected", register(guard, List.of(s, t), "rejected", true), 409);
        expect("rejected", patch("rejected", PREFIX + "rej-2", List.of(s), List.of(), true), 404);
        expect("rejected", patch("rejected", guard, List.of(), List.of(s), true), 409);
        expect("rejected", patch("rejected", guard, List.of(s), List.of(s), true), 400);
        write(Json.object("kind", "guard", "subscriberId", guard, "allowedVersions", guardVersions, "reason", "failed+rejected"));
        write(Json.object("kind", "guard", "subscriberId", rejected, "allowedVersions", new ArrayList<>(), "reason", "rejected"));
        write(Json.object("kind", "guard", "subscriberId", PREFIX + "rej-2", "allowedVersions", new ArrayList<>(),
                "reason", "rejected"));
        sale("guarded", s, t);

        // 6. The drain.
        boolean drained = drain();
        write(Json.object("kind", "end", "drained", drained, "unexpected", new ArrayList<Object>(unexpected),
                "finishedAtMs", System.currentTimeMillis()));
        log.println("change scenario: " + (drained ? "drained" : "NOT drained") + ", " + unexpected.size()
                + " unexpected answers, highest confirmed version " + confirmed.get());
        for (String line : unexpected) log.println("  unexpected: " + line);
        return drained && unexpected.isEmpty() ? 0 : 1;
    }

    /** One seeded random change of a {@code chg-NNNN} subscriber; any defined answer is acceptable. */
    private void randomChange() throws Exception {
        String id = subscriberId(1 + random.nextInt(subscribers));
        String sku = pool.get(random.nextInt(pool.size()));
        Answer answer = switch (random.nextInt(4)) {
            case 0 -> register(id, randomSkus(1 + random.nextInt(SKUS_PER_SUBSCRIBER)), "burst", false);
            case 1 -> patch("burst", id, List.of(sku), List.of(), false);
            case 2 -> patch("burst", id, List.of(pool.get(random.nextInt(pool.size()))), List.of(sku), false);
            default -> delete("burst", id, false);
        };
        if (answer.status() >= 500) unexpected.add("burst " + id + " answered " + answer.status());
    }

    // --- Subscription requests -----------------------------------------------------------------------------------

    private Answer register(String id, List<String> skus, boolean once) throws Exception {
        return register(id, skus, "setup", once);
    }

    private Answer register(String id, List<String> skus, String step, boolean once) throws Exception {
        List<Object> sorted = new ArrayList<>(new TreeSet<>(skus));
        String body = Json.compact(Json.object("subscriberId", id, "skus", sorted));
        Answer answer = send("POST", "/notifications/subscribers", body, once);
        record(step, "REGISTER", id, sorted, List.of(), answer);
        if (answer.status() == 201 || answer.status() == 200) consume(id, answer);
        return answer;
    }

    private Answer patch(String step, String id, List<String> add, List<String> remove, boolean once) throws Exception {
        String body = Json.compact(Json.object("add", new ArrayList<Object>(add), "remove", new ArrayList<Object>(remove)));
        Answer answer = send("PATCH", "/notifications/subscribers/" + id, body, once);
        record(step, "UPDATE", id, new ArrayList<>(add), new ArrayList<>(remove), answer);
        return answer;
    }

    private Answer delete(String step, String id, boolean once) throws Exception {
        Answer answer = send("DELETE", "/notifications/subscribers/" + id, null, once);
        record(step, "CANCEL", id, List.of(), List.of(), answer);
        return answer;
    }

    private void record(String step, String op, String id, List<Object> add, List<Object> remove, Answer answer)
            throws IOException {
        Long version = answer.status() >= 200 && answer.status() < 300 ? answer.version() : null;
        if (version != null) confirmed.accumulateAndGet(version, Math::max);
        write(Json.object("kind", "change", "step", step, "op", op, "subscriberId", id,
                "add", new ArrayList<>(add), "remove", new ArrayList<>(remove), "status", (long) answer.status(),
                "error", answer.error(), "version", version, "confirmedAtMs", System.currentTimeMillis()));
    }

    private void expect(String step, Answer answer, int status) {
        if (answer.status() != status) {
            unexpected.add(step + ": expected " + status + ", got " + answer.status() + " " + answer.body());
        }
    }

    private List<String> skusOf(String id) throws Exception {
        Answer answer = send("GET", "/notifications/subscribers/" + id, null, false);
        if (answer.status() != 200) throw new IllegalStateException("GET " + id + " answered " + answer.status());
        return Json.strings(Json.getList(answer.body(), "skus"));
    }

    private String notHeldBy(String id) throws Exception {
        List<String> held = skusOf(id);
        for (String sku : pool) if (!held.contains(sku)) return sku;
        throw new IllegalStateException(id + " holds the whole pool");
    }

    /** Starts consuming {@code id}'s channel once it is ready; a subscriber is consumed by one thread for the run. */
    private synchronized void consume(String id, Answer registered) throws Exception {
        if (consumers.containsKey(id)) return;
        String channel = (String) registered.body().get("channel");
        boolean ready = Boolean.TRUE.equals(registered.body().get("channelReady"));
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!ready) {
            if (System.nanoTime() > deadline) throw new IllegalStateException("channel of " + id + " never became ready");
            Thread.sleep(250);
            ready = Boolean.TRUE.equals(send("GET", "/notifications/subscribers/" + id, null, false).body().get("channelReady"));
        }
        SubscriberThread thread = new SubscriberThread(id, channel, bootstrap, new ReceiptWriter(out.resolve("receipts"), id), log);
        consumers.put(id, thread);
        Thread runner = Thread.ofPlatform().name("subscriber-" + id).start(thread);
        runners.add(runner);
    }

    // --- Sales ---------------------------------------------------------------------------------------------------

    /** A sale of {@code skus} through the public routes, recorded with the highest version confirmed before Complete. */
    private void sale(String step, String... skus) throws Exception {
        Answer started = send("POST", "/transactions", "{\"stationId\":\"change-scenario\"}", false);
        if (started.status() != 201) throw new IllegalStateException("Start answered " + started.status());
        String id = (String) started.body().get("transactionId");
        for (String sku : skus) {
            Answer scan = send("POST", "/transactions/" + id + "/items", Json.compact(Json.object("sku", sku)), false);
            if (scan.status() >= 300) throw new IllegalStateException("Scan of " + sku + " answered " + scan.status());
        }
        long before = confirmed.get();
        Answer complete = send("POST", "/transactions/" + id + "/complete", "", true);
        if (complete.status() == 200) {
            write(Json.object("kind", "sale", "step", step, "transactionId", id, "skus", new ArrayList<Object>(List.of(skus)),
                    "confirmedVersion", before, "completeStatus", 200L));
        } else {
            log.println("sale " + id + " (" + step + ") answered " + complete.status() + "; not recorded");
        }
    }

    // --- Faults, load, and drain ---------------------------------------------------------------------------------

    private void setFaults(String value) throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(faultsEndpoint))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(Json.compact(Json.object("faults", value)))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("POST " + faultsEndpoint + " answered " + response.statusCode()
                    + "; start Notification with NOTIFICATION_TEST_FAULTS=1");
        }
        write(Json.object("kind", "faults", "value", value, "atMs", System.currentTimeMillis()));
    }

    /** {@code salesSelected} of {@code GET /notifications/ops/status}: the recorded selections. */
    private long salesSelected() throws Exception {
        Answer status = send("GET", "/notifications/ops/status", null, false);
        if (status.status() != 200 || !(status.body().get("salesSelected") instanceof Long selected)) {
            throw new IllegalStateException("ops/status answered " + status.status());
        }
        return selected;
    }

    private void awaitSalesSelected(long count) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (salesSelected() < count) {
            if (System.nanoTime() > deadline) throw new IllegalStateException("the sale was never selected");
            Thread.sleep(50);
        }
    }

    private void awaitFile(String name, Duration timeout) throws InterruptedException {
        log.println("waiting for " + out.resolve(name));
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!Files.exists(out.resolve(name))) {
            if (System.nanoTime() > deadline) throw new IllegalStateException(name + " never appeared");
            Thread.sleep(200);
        }
    }

    /** Nothing pending in Notification and every channel read to its end, in two consecutive polls. */
    private boolean drain() throws Exception {
        long deadline = System.nanoTime() + DRAIN_TIMEOUT.toNanos();
        int zero = 0;
        while (System.nanoTime() < deadline) {
            Answer status = send("GET", "/notifications/ops/status", null, false);
            long pending = -1;
            if (status.status() == 200 && status.body().get("notifications") instanceof Map<?, ?> counts
                    && counts.get("pending") instanceof Long value) {
                pending = value;
            }
            long lag = 0;
            boolean known = true;
            for (SubscriberThread thread : consumers.values()) {
                StatusWriter.Stats stats = thread.stats();
                if (stats.lag() < 0) known = false;
                else lag += stats.lag();
                if (thread.failure() != null) unexpected.add("consumer " + thread.subscriberId() + " failed: " + thread.failure());
            }
            zero = pending == 0 && known && lag == 0 ? zero + 1 : 0;
            if (zero >= 2) return true;
            Thread.sleep(1000);
        }
        return false;
    }

    // --- Plumbing ------------------------------------------------------------------------------------------------

    private Answer send(String method, String path, String body, boolean once) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(30));
        if (body != null) request.header("Content-Type", "application/json");
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        int attempts = once ? 1 : 5;
        for (int attempt = 1; ; attempt++) {
            try {
                HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= 500 && attempt < attempts) {
                    Thread.sleep(200L * attempt);
                    continue;
                }
                Map<String, Object> parsed = null;
                if (!response.body().isBlank()) {
                    try {
                        parsed = Json.parseObject(response.body());
                    } catch (RuntimeException notJson) {
                        parsed = Map.of("raw", response.body());
                    }
                }
                return new Answer(response.statusCode(), parsed);
            } catch (IOException failure) {
                if (attempt >= attempts) throw failure;
                Thread.sleep(200L * attempt);
            }
        }
    }

    private synchronized void write(Map<String, Object> record) throws IOException {
        changes.write(Json.compact(record));
        changes.write('\n');
        changes.flush();
    }

    private void close() {
        for (SubscriberThread thread : consumers.values()) thread.stop();
        for (Thread runner : runners) {
            try {
                runner.join(10_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            changes.close();
        } catch (IOException ignored) {
            // Every record was flushed when written.
        }
    }

    private List<String> pickPool(List<String> catalog) {
        List<String> sorted = Assignment.sortedCatalog(catalog);
        if (sorted.size() < POOL) throw new IllegalStateException("the catalog has fewer than " + POOL + " SKUs");
        List<String> picked = new ArrayList<>(sorted);
        for (int i = 0; i < POOL; i++) {
            int j = i + random.nextInt(picked.size() - i);
            String swap = picked.get(i);
            picked.set(i, picked.get(j));
            picked.set(j, swap);
        }
        return List.copyOf(new TreeSet<>(picked.subList(0, POOL)));
    }

    private List<String> randomSkus(int count) {
        TreeSet<String> skus = new TreeSet<>();
        while (skus.size() < count) skus.add(pool.get(random.nextInt(pool.size())));
        return new ArrayList<>(skus);
    }

    static String subscriberId(int index) {
        return String.format("%s%04d", PREFIX, index);
    }

    private static int usage(String problem) {
        System.err.println(problem);
        System.err.println("Usage: ChangeScenarioMain --out=DIR [--subscribers=10] [--seed=N] [--base-url=URL]"
                + " [--bootstrap=HOST:PORT] [--with-load] [--faults-endpoint=URL]");
        return 2;
    }
}
