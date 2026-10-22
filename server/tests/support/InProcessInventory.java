package support;

import database.ConnectionPool;
import database.JdbcCatalogMetaStore;
import database.JdbcCatalogStore;
import database.JdbcCheckoutOperationStore;
import inventory.CatalogCache;
import inventory.CheckoutStockService;
import rpc.GrpcInventoryGateway;
import rpc.InventoryGrpcService;

/**
 * The real Inventory checkout path in this JVM: {@link InventoryGrpcService} over the real adapters and
 * an {@link InventoryTestSchema}, served on an in-process gRPC channel, plus Transaction's real
 * {@link GrpcInventoryGateway} on that channel. Several instances over one schema behave like several
 * Inventory processes, each with its own catalog cache and pool.
 */
public final class InProcessInventory implements AutoCloseable {
    public final ConnectionPool pool;
    public final CatalogCache catalog;
    public final CheckoutStockService stock;
    public final InventoryGrpcService service;
    private final GrpcTestSupport.InProcess server;
    private final GrpcInventoryGateway gateway;

    private InProcessInventory(ConnectionPool pool) throws Exception {
        this.pool = pool;
        catalog = new CatalogCache(new JdbcCatalogStore(pool), new JdbcCatalogMetaStore(pool));
        stock = new CheckoutStockService(new JdbcCheckoutOperationStore(pool));
        service = new InventoryGrpcService(catalog, stock);
        server = GrpcTestSupport.inProcess(service);
        gateway = new GrpcInventoryGateway(server.channel());
    }

    /** Serves the schema's own default pool. */
    public static InProcessInventory over(InventoryTestSchema schema) throws Exception {
        return new InProcessInventory(schema.pool());
    }

    /** Serves a pool of its own, for a second independent instance. */
    public static InProcessInventory overNewPool(InventoryTestSchema schema, String metricsName, int initial, int max)
            throws Exception {
        return new InProcessInventory(schema.newPool(metricsName, initial, max, null));
    }

    public GrpcInventoryGateway gateway() {
        return gateway;
    }

    public io.grpc.ManagedChannel channel() {
        return server.channel();
    }

    @Override
    public void close() throws InterruptedException {
        server.close();
    }
}
