package support;

import transaction.InventoryGateway;

/**
 * A catalog-only {@link InventoryGateway} for unit and in-process tests (006 T074): every call fails the
 * test, for services that must never ask Inventory for the catalog. Checkout no longer uses the gateway;
 * its tests answer checkout requests with {@link FakeInventoryReactor}. The class goes with the gateway in
 * T113 (US4).
 */
public final class FakeInventoryGateway implements InventoryGateway {
    private FakeInventoryGateway() { }

    /** Fails the test if anything asks Inventory for its catalog. */
    public static FakeInventoryGateway failing() {
        return new FakeInventoryGateway();
    }

    @Override
    public CatalogVersion catalogVersion() {
        throw new AssertionError("Inventory's catalog version was requested");
    }

    @Override
    public Catalog catalog() {
        throw new AssertionError("Inventory's catalog was requested");
    }
}
