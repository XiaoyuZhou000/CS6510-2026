package integration;

import api.ApiErrorMapper;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.InMemoryEventBus;
import support.InProcessInventory;
import support.InProcessTransaction;
import support.InventoryTestSchema;
import support.TestEvents;
import support.TransactionTestSchema;
import transaction.ProductCatalogUnavailableFailure;
import transaction.TransactionOperations;

import java.math.BigDecimal;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog replication over events on {@link InMemoryEventBus} (US4 scenarios 1–4, D9): the real Inventory
 * publisher records its catalog in its outbox, and the real Transaction catalog consumer replays the topic
 * from offset 0 into its durable replica. No synchronous call between the services exists.
 */
class CatalogReplicationTest {
    private static final Duration CONVERGE = Duration.ofSeconds(10);

    @Test
    void aFreshSeedReachesTransactionsReplicaWithNoSynchronousCall() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 1);
        try (InventoryTestSchema inventories = InventoryTestSchema.create();
             TransactionTestSchema transactions = TransactionTestSchema.create()) {
            String version = inventories.seed(5, 100);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).bus(bus, names)
                    .catalogFromEvents().build();
            assertFalse(transaction.replica.usable(), "nothing was published yet");

            InProcessInventory inventory = InProcessInventory.over(inventories, bus, names);
            assertEquals(version, inventory.publishCatalog().orElseThrow().catalogVersion());
            awaitVersion(transaction, version);

            assertEquals(5, transactions.queryLong("SELECT COUNT(*) FROM product_replica WHERE catalog_version = ?", version));
            assertEquals(version, transactions.queryString("SELECT catalog_version FROM product_replica_meta WHERE meta_key = 1"));
            String tx = transaction.start();
            TransactionOperations.ScanView scanned = transaction.scan(tx, InventoryTestSchema.sku(3));
            assertEquals(InventoryTestSchema.price(3), scanned.unitPrice());
            assertThrows(ClassNotFoundException.class, () -> Class.forName("transaction.InventoryGateway"),
                    "Transaction has no route to Inventory other than events");
            assertEquals(6, bus.endOffset(names.catalog(), 0), "five items and one snapshot marker on one partition");
        }
    }

    @Test
    void aTransactionStartedAfterThePublicationStillConverges() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 2);
        try (InventoryTestSchema inventories = InventoryTestSchema.create();
             TransactionTestSchema transactions = TransactionTestSchema.create()) {
            String version = inventories.seed(4, 100);
            InProcessInventory inventory = InProcessInventory.over(inventories, bus, names);
            inventory.publishCatalog();
            awaitPublished(inventories);
            inventory.close();

            InProcessTransaction late = InProcessTransaction.over(transactions).bus(bus, names).catalogFromEvents().build();
            awaitVersion(late, version);
            assertEquals(4, late.replica.size(), "the compacted topic is replayed from offset 0, no request event needed");
        }
    }

    @Test
    void aReseedWithADifferentCatalogConvergesAndALateOlderRecordNeverOverwritesIt() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 3);
        try (InventoryTestSchema inventories = InventoryTestSchema.create();
             TransactionTestSchema transactions = TransactionTestSchema.create()) {
            String first = inventories.seed(5, 100);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).bus(bus, names)
                    .catalogFromEvents().build();
            InProcessInventory inventory = InProcessInventory.over(inventories, bus, names);
            inventory.publishCatalog();
            awaitVersion(transaction, first);
            inventory.close();

            // Reseed while Inventory is stopped: one price changes and one product is removed.
            inventories.update("UPDATE catalog_item SET price = 9.99 WHERE sku = ?", InventoryTestSchema.sku(1));
            inventories.update("DELETE FROM catalog_item WHERE sku = ?", InventoryTestSchema.sku(5));
            String second = inventories.newCatalogVersion();
            inventory = InProcessInventory.over(inventories, bus, names);
            inventory.publishCatalog();
            awaitVersion(transaction, second);
            assertEquals(new BigDecimal("9.99"), transaction.replica.lookup(InventoryTestSchema.sku(1)).orElseThrow().price());
            assertTrue(transaction.replica.lookup(InventoryTestSchema.sku(5)).isEmpty(), "a removed product leaves the replica");
            assertEquals(4, transactions.queryLong("SELECT COUNT(*) FROM product_replica"));

            // A late record of the older version, and its marker, change nothing.
            bus.send(names.catalog(), InventoryTestSchema.sku(1),
                    TestEvents.catalogItem(InventoryTestSchema.sku(1), "Item 1", "0.01", first)).join();
            bus.send(names.catalog(), "__snapshot__", TestEvents.catalogSnapshot(first, 5)).join();
            long end = bus.endOffset(names.catalog(), 0);
            awaitConsumed(transaction, end);
            assertEquals(second, transaction.replica.version());
            assertEquals(new BigDecimal("9.99"), transaction.replica.lookup(InventoryTestSchema.sku(1)).orElseThrow().price());
            assertEquals(second, transactions.queryString("SELECT catalog_version FROM product_replica_meta WHERE meta_key = 1"));

            // A Transaction restarted now replays every version from offset 0 and ends at the newest.
            transaction.close();
            InProcessTransaction restarted = InProcessTransaction.over(transactions).bus(bus, names).catalogFromEvents().build();
            awaitConsumed(restarted, end);
            assertEquals(second, restarted.replica.version());
            assertEquals(4, restarted.replica.size());
        }
    }

    @Test
    void aReseedWithTheSameCatalogLeavesTheReplicaUnchanged() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 4);
        try (InventoryTestSchema inventories = InventoryTestSchema.create();
             TransactionTestSchema transactions = TransactionTestSchema.create()) {
            String version = inventories.seed(3, 100);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).bus(bus, names)
                    .catalogFromEvents().build();
            InProcessInventory inventory = InProcessInventory.over(inventories, bus, names);
            inventory.publishCatalog();
            awaitVersion(transaction, version);
            String refreshed = transactions.queryString("SELECT refreshed_at FROM product_replica_meta WHERE meta_key = 1");
            inventory.close();

            // Every Inventory startup republishes; the same version is a no-op for Transaction.
            inventory = InProcessInventory.over(inventories, bus, names);
            inventory.publishCatalog();
            awaitPublished(inventories);
            assertEquals(8, bus.endOffset(names.catalog(), 0), "two publications of three items and a marker");
            awaitConsumed(transaction, 8);
            assertEquals(version, transaction.replica.version());
            assertEquals(refreshed, transactions.queryString("SELECT refreshed_at FROM product_replica_meta WHERE meta_key = 1"),
                    "the replica was not rewritten");
        }
    }

    @Test
    void withTheBrokerPausedAndInventoryStoppedARestartedTransactionServesTheDurableReplica() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 5);
        try (InventoryTestSchema inventories = InventoryTestSchema.create();
             TransactionTestSchema transactions = TransactionTestSchema.create()) {
            String version = inventories.seed(3, 100);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).bus(bus, names)
                    .catalogFromEvents().build();
            InProcessInventory inventory = InProcessInventory.over(inventories, bus, names);
            inventory.publishCatalog();
            awaitVersion(transaction, version);
            inventory.close();
            transaction.close();

            bus.pauseBroker(true);
            InProcessTransaction restarted = InProcessTransaction.over(transactions).bus(bus, names)
                    .catalogFromEvents().build();
            assertTrue(restarted.replica.usable(), "the durable replica is loaded before the topic is read");
            assertEquals(version, restarted.replica.version());
            String tx = restarted.start();
            assertEquals(InventoryTestSchema.price(2),
                    restarted.scan(tx, InventoryTestSchema.sku(2)).unitPrice());
            bus.pauseBroker(false);
        }
    }

    @Test
    void withNoReplicaScanAnswersInternalError() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 6);
        try (TransactionTestSchema transactions = TransactionTestSchema.create()) {
            InProcessTransaction transaction = InProcessTransaction.over(transactions).bus(bus, names)
                    .catalogFromEvents().build();
            assertFalse(transaction.replica.usable());
            String tx = transaction.start();
            ProductCatalogUnavailableFailure unavailable = assertThrows(ProductCatalogUnavailableFailure.class,
                    () -> transaction.scan(tx, InventoryTestSchema.sku(1)));
            ApiErrorMapper.ApiError error = ApiErrorMapper.map(unavailable);
            assertEquals(500, error.httpStatus());
            assertEquals("INTERNAL_ERROR", error.error(), "FR-030");
            assertEquals(0, transactions.scanCount(), "nothing was accepted");
        }
    }

    private static void awaitVersion(InProcessTransaction transaction, String version) throws InterruptedException {
        long deadline = System.nanoTime() + CONVERGE.toNanos();
        while (!version.equals(transaction.replica.version())) {
            if (System.nanoTime() > deadline) {
                fail("The replica did not reach version " + version + "; it is at " + transaction.replica.version());
            }
            Thread.sleep(20);
        }
    }

    /** Waits until the catalog consumer has handled every record below {@code endOffset}. */
    private static void awaitConsumed(InProcessTransaction transaction, long endOffset) throws InterruptedException {
        long deadline = System.nanoTime() + CONVERGE.toNanos();
        while (transaction.catalogHandler.nextOffset() < endOffset) {
            if (System.nanoTime() > deadline) {
                fail("The catalog consumer stopped at " + transaction.catalogHandler.nextOffset() + " of " + endOffset);
            }
            Thread.sleep(20);
        }
    }

    private static void awaitPublished(InventoryTestSchema inventories) throws Exception {
        long deadline = System.nanoTime() + CONVERGE.toNanos();
        while (inventories.unpublishedOutboxCount() > 0) {
            if (System.nanoTime() > deadline) fail("The catalog outbox rows were never published");
            Thread.sleep(20);
        }
    }
}
