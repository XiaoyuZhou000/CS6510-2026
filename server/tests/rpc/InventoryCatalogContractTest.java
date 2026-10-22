package rpc;

import contracts.inventory.v1.CatalogProduct;
import contracts.inventory.v1.GetCatalogRequest;
import contracts.inventory.v1.GetCatalogResponse;
import contracts.inventory.v1.GetCatalogVersionRequest;
import contracts.inventory.v1.GetCatalogVersionResponse;
import contracts.inventory.v1.InventoryServiceGrpc;
import database.JdbcCatalogMetaStore;
import database.JdbcCatalogStore;
import database.JdbcCheckoutOperationStore;
import database.JdbcInventoryStore;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import inventory.CatalogCache;
import inventory.CatalogOperations;
import inventory.CheckoutStockService;
import inventory.StoreQueryService;
import org.junit.jupiter.api.Test;
import support.GrpcTestSupport;
import support.InventoryTestSchema;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** InventoryService catalog replication over a real adapter and a disposable schema (R4, R8; FR-013). */
class InventoryCatalogContractTest {
    private static final int PRODUCTS = 2000;

    @Test
    void catalogVersionAndSnapshotComeFromTheCachedSeedInSkuOrderWithExactCents() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            String version = schema.seed(PRODUCTS, 10_000);
            CatalogCache cache = cache(schema);
            try (GrpcTestSupport.InProcess server = GrpcTestSupport.inProcess(service(schema, cache))) {
                var stub = stub(server.channel());

                GetCatalogVersionResponse current = stub.getCatalogVersion(GetCatalogVersionRequest.getDefaultInstance());
                assertEquals(version, current.getCatalogVersion());
                assertEquals(PRODUCTS, current.getProductCount());

                GetCatalogResponse catalog = stub.getCatalog(GetCatalogRequest.getDefaultInstance());
                assertEquals(version, catalog.getCatalogVersion(), "a snapshot and its version come from one load");
                assertEquals(PRODUCTS, catalog.getProductsCount());
                for (int n = 1; n <= PRODUCTS; n++) {
                    CatalogProduct product = catalog.getProducts(n - 1);
                    assertEquals(InventoryTestSchema.sku(n), product.getSku(), "ascending SKU order");
                    assertEquals("Item " + n, product.getName());
                    assertEquals(InventoryTestSchema.price(n).movePointRight(2).longValueExact(), product.getPriceCents());
                }
                assertEquals(85, catalog.getProducts(0).getPriceCents(), "SKU-000001 costs 0.85, exactly 85 cents");
            }
        }
    }

    @Test
    void aNewCatalogVersionReloadsTheCacheThatGetCatalogAndListItemsShare() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            String original = schema.seed(PRODUCTS, 10_000);
            CatalogCache cache = cache(schema);
            StoreQueryService queries = new StoreQueryService(cache, new JdbcInventoryStore(schema.pool()));
            try (GrpcTestSupport.InProcess server = GrpcTestSupport.inProcess(service(schema, cache))) {
                var stub = stub(server.channel());
                assertEquals("Item 2", stub.getCatalog(GetCatalogRequest.getDefaultInstance()).getProducts(1).getName());

                schema.update("UPDATE catalog_item SET name = 'Renamed Item 2' WHERE sku = ?", InventoryTestSchema.sku(2));
                String reseeded = UUID.randomUUID().toString();
                schema.update("UPDATE catalog_meta SET catalog_version = ? WHERE meta_key = 1", reseeded);

                assertEquals("Item 2", stub.getCatalog(GetCatalogRequest.getDefaultInstance()).getProducts(1).getName(),
                        "GetCatalog serves the cache until a version check sees the change");
                assertEquals("Item 2", name(queries.listItems(), 1), "/items serves the same cache");

                GetCatalogVersionResponse changed = stub.getCatalogVersion(GetCatalogVersionRequest.getDefaultInstance());
                assertNotEquals(original, changed.getCatalogVersion());
                assertEquals(reseeded, changed.getCatalogVersion());

                GetCatalogResponse reloaded = stub.getCatalog(GetCatalogRequest.getDefaultInstance());
                assertEquals(reseeded, reloaded.getCatalogVersion());
                assertEquals("Renamed Item 2", reloaded.getProducts(1).getName());
                assertEquals("Renamed Item 2", name(queries.listItems(), 1), "listItems shows the reloaded cache");
                assertEquals(PRODUCTS, queries.listItems().size());
            }
        }
    }

    @Test
    void theSameServiceAnswersOverARealPort() throws Exception {
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            String version = schema.seed(5, 10);
            GrpcServers.Running server = GrpcServers.start(GrpcTestSupport.freePort(), service(schema, cache(schema)));
            ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
            try {
                assertEquals(version, stub(channel).getCatalogVersion(GetCatalogVersionRequest.getDefaultInstance())
                        .getCatalogVersion());
                assertEquals(5, stub(channel).getCatalog(GetCatalogRequest.getDefaultInstance()).getProductsCount());
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.stop(Duration.ofSeconds(5));
            }
        }
    }

    /** Checkout calls are covered by InventoryCheckoutContractTest; here they only complete the service. */
    private static InventoryGrpcService service(InventoryTestSchema schema, CatalogCache cache) throws Exception {
        return new InventoryGrpcService(cache, new CheckoutStockService(new JdbcCheckoutOperationStore(schema.pool())));
    }

    private static CatalogCache cache(InventoryTestSchema schema) throws Exception {
        return new CatalogCache(new JdbcCatalogStore(schema.pool()), new JdbcCatalogMetaStore(schema.pool()));
    }

    private static InventoryServiceGrpc.InventoryServiceBlockingStub stub(io.grpc.Channel channel) {
        return InventoryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    private static String name(List<CatalogOperations.CatalogItemView> items, int index) {
        return items.get(index).name();
    }
}
