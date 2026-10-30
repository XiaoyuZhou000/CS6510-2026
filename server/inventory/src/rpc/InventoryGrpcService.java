package rpc;

import contracts.inventory.v1.CatalogProduct;
import contracts.inventory.v1.GetCatalogRequest;
import contracts.inventory.v1.GetCatalogResponse;
import contracts.inventory.v1.GetCatalogVersionRequest;
import contracts.inventory.v1.GetCatalogVersionResponse;
import contracts.inventory.v1.InventoryServiceGrpc;
import database.StoreFailure;
import inventory.CatalogCache;
import inventory.CatalogOperations;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * Translates InventoryService calls to Inventory's business operations (contracts/grpc-internal-contracts.md),
 * for what still crosses gRPC during feature 006: catalog replication, served from {@link CatalogCache}, until
 * the catalog moves to events (T111). Checkout and the reserved restoration run over events (D8), so the
 * checkout and restoration RPCs answer UNIMPLEMENTED. Error statuses are reserved for faults, and their
 * descriptions carry no database details.
 */
public final class InventoryGrpcService extends InventoryServiceGrpc.InventoryServiceImplBase {
    private final CatalogCache catalog;

    public InventoryGrpcService(CatalogCache catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    public void getCatalogVersion(GetCatalogVersionRequest request, StreamObserver<GetCatalogVersionResponse> response) {
        try {
            CatalogCache.Version version = catalog.currentVersion();
            if (version.catalogVersion() == null) {
                response.onError(Status.UNAVAILABLE.withDescription("The catalog is not seeded").asRuntimeException());
                return;
            }
            response.onNext(GetCatalogVersionResponse.newBuilder().setCatalogVersion(version.catalogVersion())
                    .setProductCount(version.productCount()).build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        }
    }

    @Override
    public void getCatalog(GetCatalogRequest request, StreamObserver<GetCatalogResponse> response) {
        try {
            CatalogCache.Snapshot snapshot = catalog.snapshot();
            if (snapshot.version() == null) {
                response.onError(Status.UNAVAILABLE.withDescription("The catalog is not seeded").asRuntimeException());
                return;
            }
            GetCatalogResponse.Builder catalogResponse = GetCatalogResponse.newBuilder()
                    .setCatalogVersion(snapshot.version());
            for (CatalogOperations.CatalogItemView product : snapshot.products()) {
                catalogResponse.addProducts(CatalogProduct.newBuilder().setSku(product.sku()).setName(product.name())
                        .setPriceCents(product.price().movePointRight(2).longValueExact()));
            }
            response.onNext(catalogResponse.build());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure).asRuntimeException());
        }
    }

    /** The contract's status table; no description carries database details. */
    private static Status status(RuntimeException failure) {
        if (failure instanceof StoreFailure store) {
            return store.contended()
                    ? Status.ABORTED.withDescription("Inventory's stock rows stayed contended")
                    : Status.UNAVAILABLE.withDescription("Inventory's database is unavailable");
        }
        return Status.INTERNAL.withDescription("Inventory failed to answer");
    }
}
