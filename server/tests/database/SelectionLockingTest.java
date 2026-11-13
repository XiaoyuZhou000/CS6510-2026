package database;

import database.NotificationJson.Line;
import database.SelectionStore.Plan;
import database.SelectionStore.Sale;
import database.SelectionStore.Selected;
import database.SubscriptionStore.Applied;
import database.SubscriptionStore.ChangeResult;
import database.SubscriptionStore.ChangeRow;
import events.EventIds;
import faults.FaultHooks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import support.NotificationTestSchema;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The clock lock between selections and subscription changes (007 T083, research N4, contracts/notification-delivery.md):
 * each selection and each change runs on its own pooled connection of one test schema. A selection holds the clock
 * {@code FOR SHARE} and a change takes it {@code FOR UPDATE}, so a selection sees exactly the committed history up to
 * the version it records, changes commit one at a time, and selections never wait for each other.
 */
class SelectionLockingTest {
    private static final Instant NOW = Instant.parse("2026-11-12T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @AfterEach
    void clearFaults() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void aSelectionHoldingTheClockForShareBlocksAChangeUntilItCommits() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            subscriptions.register("s1", skus("A"), "t1", null, NOW);
            JdbcSelectionStore selections = new JdbcSelectionStore(schema.pool(), CLOCK);
            CompletableFuture<Void> holding = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<SelectionStore.Result> selection = CompletableFuture.supplyAsync(() ->
                    selections.select(sale("tx-1"), (version, changes) -> {
                        holding.complete(null);
                        release.join();
                        return new Plan(List.of(), 0);
                    }));
            holding.get(10, TimeUnit.SECONDS);
            CompletableFuture<ChangeResult> change =
                    CompletableFuture.supplyAsync(() -> subscriptions.update("s1", skus("B"), skus(), null, NOW));
            assertThrows(TimeoutException.class, () -> change.get(500, TimeUnit.MILLISECONDS),
                    "the update waits for the selection's shared lock");
            assertEquals(1, schema.subscriptionVersion());
            release.complete(null);
            assertEquals(1, assertInstanceOf(Selected.class, selection.get(10, TimeUnit.SECONDS)).subscriptionVersion(),
                    "the selection records the version it locked, without the waiting change");
            assertEquals(2, assertInstanceOf(Applied.class, change.get(10, TimeUnit.SECONDS)).change().version());
        }
    }

    @Test
    void aSelectionStartedAfterAChangeCommitsRecordsItsVersionAndSeesItsRows() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            subscriptions.register("s1", skus("A"), "t1", null, NOW);
            subscriptions.update("s1", skus("B"), skus("A"), null, NOW);
            subscriptions.cancel("s1", NOW);
            JdbcSelectionStore selections = new JdbcSelectionStore(schema.pool(), CLOCK);
            AtomicReference<List<ChangeRow>> seen = new AtomicReference<>();
            Selected selected = assertInstanceOf(Selected.class, selections.select(sale("tx-1"), (version, changes) -> {
                seen.set(changes.changesAfter(0, version));
                return new Plan(List.of(), 0);
            }));
            assertEquals(3, selected.subscriptionVersion());
            assertEquals(List.of(1L, 2L, 3L), seen.get().stream().map(ChangeRow::version).toList(),
                    "the selection reads every committed row up to its version");
            assertEquals(List.of("B"), seen.get().get(1).resultingSkus());
            assertEquals(SubscriptionStore.ChangeType.CANCEL, seen.get().get(2).type());
        }
    }

    @Test
    void twoConcurrentChangesGetConsecutiveVersionsInCommitOrder() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            subscriptions.register("s1", skus("A"), "t1", null, NOW);
            subscriptions.register("s2", skus("A"), "t2", null, NOW);
            // Each change sleeps 400 ms between its writes and its commit, while it holds the clock FOR UPDATE.
            System.setProperty(FaultHooks.PROPERTY, "notification.subscription.beforeCommit:delay:400");
            AtomicLong firstDone = new AtomicLong();
            AtomicLong secondDone = new AtomicLong();
            CompletableFuture<ChangeResult> first = CompletableFuture.supplyAsync(() -> {
                ChangeResult result = subscriptions.update("s1", skus("B"), skus(), null, NOW);
                firstDone.set(System.nanoTime());
                return result;
            });
            Thread.sleep(150);
            CompletableFuture<ChangeResult> second = CompletableFuture.supplyAsync(() -> {
                ChangeResult result = subscriptions.cancel("s2", NOW);
                secondDone.set(System.nanoTime());
                return result;
            });
            long firstVersion = assertInstanceOf(Applied.class, first.get(10, TimeUnit.SECONDS)).change().version();
            long secondVersion = assertInstanceOf(Applied.class, second.get(10, TimeUnit.SECONDS)).change().version();
            assertEquals(3, firstVersion, "the change that took the clock first commits first");
            assertEquals(4, secondVersion, "the waiting change takes the next version");
            assertTrue(secondDone.get() > firstDone.get(), "version order is commit order");
            assertEquals(4, schema.subscriptionVersion());
            assertEquals(4, schema.subscriptionChanges());
        }
    }

    @Test
    void twoSelectionsDoNotBlockEachOther() throws Exception {
        try (NotificationTestSchema schema = NotificationTestSchema.create()) {
            JdbcSubscriptionStore subscriptions = new JdbcSubscriptionStore(schema.pool());
            subscriptions.register("s1", skus("A"), "t1", null, NOW);
            JdbcSelectionStore selections = new JdbcSelectionStore(schema.pool(), CLOCK);
            CompletableFuture<Void> holding = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<SelectionStore.Result> slow = CompletableFuture.supplyAsync(() ->
                    selections.select(sale("tx-1"), (version, changes) -> {
                        holding.complete(null);
                        release.join();
                        return new Plan(List.of(), 0);
                    }));
            holding.get(10, TimeUnit.SECONDS);
            try {
                SelectionStore.Result fast = CompletableFuture.supplyAsync(() ->
                        selections.select(sale("tx-2"), (version, changes) -> new Plan(List.of(), 0))).get(5, TimeUnit.SECONDS);
                assertEquals(1, assertInstanceOf(Selected.class, fast).subscriptionVersion(),
                        "a second selection shares the clock lock and commits while the first still holds it");
            } finally {
                release.complete(null);
            }
            assertInstanceOf(Selected.class, slow.get(10, TimeUnit.SECONDS));
            assertEquals(2, schema.saleSelections());
        }
    }

    private static Sale sale(String id) {
        return new Sale(id, EventIds.saleEventId(id), NOW.minusSeconds(1), List.of(new Line("A", 1)));
    }

    private static TreeSet<String> skus(String... skus) {
        return new TreeSet<>(List.of(skus));
    }
}
