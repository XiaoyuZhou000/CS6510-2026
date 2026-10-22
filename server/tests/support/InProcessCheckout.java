package support;

import transaction.TransactionOperations;

/**
 * Transaction and Inventory in this JVM over their own disposable schemas, wired as in production
 * except for HTTP and the transport: Complete reaches the real Inventory checkout path over an
 * in-process gRPC channel. The product replica is seeded with Inventory's catalog and version.
 */
public final class InProcessCheckout implements AutoCloseable {
    public final TransactionTestSchema transactionSchema;
    public final InventoryTestSchema inventorySchema;
    public final InProcessInventory inventory;
    public final InProcessTransaction transaction;

    private InProcessCheckout(TransactionTestSchema transactionSchema, InventoryTestSchema inventorySchema,
                              InProcessInventory inventory, InProcessTransaction transaction) {
        this.transactionSchema = transactionSchema;
        this.inventorySchema = inventorySchema;
        this.inventory = inventory;
        this.transaction = transaction;
    }

    /** {@code products} deterministic products ({@code SKU-000001}..), each with {@code stock} units. */
    public static InProcessCheckout start(int products, int stock) throws Exception {
        TransactionTestSchema transactions = TransactionTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
        InventoryTestSchema inventories = null;
        InProcessInventory inventory = null;
        try {
            inventories = InventoryTestSchema.create(TestSchema.Options.defaults().pool(4, 24));
            transactions.seedReplica(products, inventories.seed(products, stock));
            inventory = InProcessInventory.over(inventories);
            InProcessTransaction transaction = InProcessTransaction.over(transactions).pool(4, 24)
                    .inventory(inventory.gateway()).build();
            return new InProcessCheckout(transactions, inventories, inventory, transaction);
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
            inventory.close();
        }
    }
}
