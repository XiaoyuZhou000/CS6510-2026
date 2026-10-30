package support;

import events.TopicNames;
import transaction.TransactionOperations;

/**
 * Transaction and Inventory in this JVM over their own disposable schemas, wired as in production except
 * for HTTP and the transport (006 T059): Complete records {@code CheckoutRequested}, and the real Inventory
 * checkout path consumes it and answers with its outcome event, over one shared {@link InMemoryEventBus}.
 * The product replica is seeded with Inventory's catalog and version.
 */
public final class InProcessCheckout implements AutoCloseable {
    public final TransactionTestSchema transactionSchema;
    public final InventoryTestSchema inventorySchema;
    public final TopicNames names;
    public final InMemoryEventBus bus;
    public final InProcessInventory inventory;
    public final InProcessTransaction transaction;

    private InProcessCheckout(TransactionTestSchema transactionSchema, InventoryTestSchema inventorySchema,
                              TopicNames names, InMemoryEventBus bus, InProcessInventory inventory,
                              InProcessTransaction transaction) {
        this.transactionSchema = transactionSchema;
        this.inventorySchema = inventorySchema;
        this.names = names;
        this.bus = bus;
        this.inventory = inventory;
        this.transaction = transaction;
    }

    /** {@code products} deterministic products ({@code SKU-000001}..), each with {@code stock} units. */
    public static InProcessCheckout start(int products, int stock) throws Exception {
        return start(products, stock, 6510);
    }

    /** As {@link #start(int, int)}, with the bus's seed for its fault controls. */
    public static InProcessCheckout start(int products, int stock, long seed) throws Exception {
        TransactionTestSchema transactions = TransactionTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
        InventoryTestSchema inventories = null;
        InProcessInventory inventory = null;
        try {
            inventories = InventoryTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
            transactions.seedReplica(products, inventories.seed(products, stock));
            TopicNames names = TopicNames.unprefixed();
            InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, seed);
            inventory = InProcessInventory.over(inventories, bus, names);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).pool(4, 24).bus(bus, names).build();
            return new InProcessCheckout(transactions, inventories, names, bus, inventory, transaction);
        } catch (Exception failure) {
            if (inventory != null) inventory.close();
            if (inventories != null) inventories.close();
            transactions.close();
            throw failure;
        }
    }

    public TransactionOperations checkout() {
        return transaction.service;
    }

    public int stock(String sku) throws Exception {
        return inventorySchema.stock(sku);
    }

    @Override
    public void close() throws Exception {
        try (transactionSchema; inventorySchema) {
            transaction.close();
            inventory.close();
        }
    }
}
