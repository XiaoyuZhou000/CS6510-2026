package database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** JDBC adapter for {@code product_replica} and its {@code product_replica_meta} singleton (R4). */
public final class JdbcProductReplicaStore implements ProductReplicaStore {
    /** One statement, so the version and its products come from one committed snapshot. */
    private static final String LOAD = """
            SELECT m.catalog_version, m.product_count, p.sku, p.name, p.price
            FROM product_replica_meta m
            LEFT JOIN product_replica p ON p.catalog_version = m.catalog_version
            WHERE m.meta_key = 1
            ORDER BY p.sku
            """;
    private static final String INSERT =
            "INSERT INTO product_replica (sku, name, price, catalog_version) VALUES (?, ?, ?, ?)";
    private static final String UPDATE_META =
            "UPDATE product_replica_meta SET catalog_version = ?, product_count = ?, refreshed_at = ? WHERE meta_key = 1";

    private final ConnectionPool pool;

    public JdbcProductReplicaStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public Optional<Replica> load() {
        return JdbcUnitOfWork.read(pool, "transaction.replica.load", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(LOAD);
                 ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || rows.getString(1) == null) return Optional.empty();
                String version = rows.getString(1);
                int expected = rows.getInt(2);
                List<ReplicaProduct> products = new ArrayList<>(expected);
                do {
                    if (rows.getString(3) != null) {
                        products.add(new ReplicaProduct(rows.getString(3), rows.getString(4), rows.getBigDecimal(5)));
                    }
                } while (rows.next());
                if (products.size() != expected) {
                    throw new StoreFailure("The product replica holds " + products.size() + " products for version "
                            + version + ", not " + expected);
                }
                return Optional.of(new Replica(version, products));
            }
        });
    }

    /** DELETE, one batched multi-row INSERT, and the meta UPDATE share one commit. */
    @Override
    public void replace(String catalogVersion, List<ReplicaProduct> products) {
        Objects.requireNonNull(catalogVersion, "catalogVersion");
        List<ReplicaProduct> rows = List.copyOf(products);
        JdbcUnitOfWork.inTransaction(pool, "transaction.replica.replace", connection -> {
            try (Statement delete = connection.createStatement()) {
                delete.executeUpdate("DELETE FROM product_replica");
            }
            if (!rows.isEmpty()) {
                try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
                    for (ReplicaProduct product : rows) {
                        insert.setString(1, product.sku());
                        insert.setString(2, product.name());
                        insert.setBigDecimal(3, product.price());
                        insert.setString(4, catalogVersion);
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
            }
            try (PreparedStatement meta = connection.prepareStatement(UPDATE_META)) {
                meta.setString(1, catalogVersion);
                meta.setInt(2, rows.size());
                meta.setTimestamp(3, Timestamp.from(Instant.now()));
                if (meta.executeUpdate() != 1) throw new SQLException("The product_replica_meta singleton is missing");
            }
            return null;
        });
    }
}
