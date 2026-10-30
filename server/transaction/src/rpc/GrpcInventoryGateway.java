package rpc;

import contracts.inventory.v1.CatalogProduct;
import contracts.inventory.v1.GetCatalogRequest;
import contracts.inventory.v1.GetCatalogResponse;
import contracts.inventory.v1.GetCatalogVersionRequest;
import contracts.inventory.v1.GetCatalogVersionResponse;
import contracts.inventory.v1.InventoryServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import transaction.DependencyUnavailableFailure;
import transaction.InventoryGateway;
import transaction.ProductCatalogReplica;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link InventoryGateway} over Inventory's gRPC interface, on one reused channel (R2): catalog replication
 * only, until the catalog moves to events (006 T110). Deadlines are 2 s for {@code GetCatalogVersion} and
 * 5 s for {@code GetCatalog}; statuses are read per contracts/grpc-internal-contracts.md.
 */
public final class GrpcInventoryGateway implements InventoryGateway {
    static final Duration VERSION_DEADLINE = Duration.ofSeconds(2);
    static final Duration CATALOG_DEADLINE = Duration.ofSeconds(5);

    private final InventoryServiceGrpc.InventoryServiceBlockingStub stub;

    public GrpcInventoryGateway(Channel channel) {
        this.stub = InventoryServiceGrpc.newBlockingStub(Objects.requireNonNull(channel, "channel"));
    }

    /** The gateway on the process-wide channel for {@code host:port}. */
    public static GrpcInventoryGateway forTarget(String target) {
        return new GrpcInventoryGateway(GrpcChannels.forTarget(target));
    }

    @Override
    public CatalogVersion catalogVersion() {
        try {
            GetCatalogVersionResponse response = stub
                    .withDeadlineAfter(VERSION_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .getCatalogVersion(GetCatalogVersionRequest.getDefaultInstance());
            return new CatalogVersion(response.getCatalogVersion(), response.getProductCount());
        } catch (StatusRuntimeException failure) {
            throw translate("GetCatalogVersion", failure);
        }
    }

    @Override
    public Catalog catalog() {
        try {
            GetCatalogResponse response = stub
                    .withDeadlineAfter(CATALOG_DEADLINE.toMillis(), TimeUnit.MILLISECONDS)
                    .getCatalog(GetCatalogRequest.getDefaultInstance());
            List<ProductCatalogReplica.Product> products = new ArrayList<>(response.getProductsCount());
            for (CatalogProduct product : response.getProductsList()) {
                products.add(new ProductCatalogReplica.Product(product.getSku(), product.getName(),
                        BigDecimal.valueOf(product.getPriceCents(), 2)));
            }
            return new Catalog(response.getCatalogVersion(), products);
        } catch (StatusRuntimeException failure) {
            throw translate("GetCatalog", failure);
        }
    }

    private static RuntimeException translate(String call, StatusRuntimeException failure) {
        if (RpcStatusClassifier.classify(failure) == RpcStatusClassifier.Kind.UNCERTAIN) {
            return new DependencyUnavailableFailure("Inventory " + call + " failed: " + failure.getStatus().getCode(),
                    failure);
        }
        return new IllegalStateException("Inventory refused " + call + ": " + failure.getStatus().getCode(), failure);
    }
}
