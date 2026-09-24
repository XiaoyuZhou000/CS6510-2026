package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JdbcStoreAdaptersTest {
    @Test
    void catalogTransactionAndInventoryAdaptersMapRowsThroughTypedContracts() throws Exception {
        try (Fixture fixture = Fixture.open()) {
            assertEquals(List.of("A", "B"), fixture.catalog.loadAll().stream()
                    .map(CatalogStore.CatalogItem::sku).toList());
            assertEquals(new BigDecimal("1.25"), fixture.catalog.loadAll().getFirst().price());

            fixture.transactions.insertOpen("tx-adapter", "station-1");
            TransactionStore.TransactionRecord transaction =
                    fixture.transactions.findById("tx-adapter").orElseThrow();
            assertEquals(TransactionStore.TransactionStatus.OPEN, transaction.status());
            assertEquals(new BigDecimal("0.00"), transaction.totalAmount());
            assertEquals(0, transaction.itemCount());
            assertTrue(transaction.startedAt().isBefore(Instant.now().plusSeconds(1)));

            List<InventoryStore.LowStockRecord> defaults = fixture.inventory.findLowStock(null);
            assertEquals(List.of("A"), defaults.stream().map(InventoryStore.LowStockRecord::sku).toList());
            assertEquals(2, defaults.getFirst().threshold());

            List<InventoryStore.LowStockRecord> override = fixture.inventory.findLowStock(5);
            assertEquals(List.of("A", "B"), override.stream()
                    .map(InventoryStore.LowStockRecord::sku).toList());
            assertTrue(override.stream().allMatch(row -> row.threshold() == 5));
        }
    }

    @Test
    void sqlFailuresAreWrappedAsTransportNeutralStoreFailures() throws Exception {
        try (Fixture fixture = Fixture.open()) {
            fixture.execute("DROP TABLE " + fixture.database + ".catalog_item");
            fixture.execute("DROP TABLE " + fixture.database + ".`transaction`");
            fixture.execute("DROP TABLE " + fixture.database + ".inventory");

            StoreFailure catalogFailure = assertThrows(StoreFailure.class, fixture.catalog::loadAll);
            StoreFailure transactionFailure = assertThrows(StoreFailure.class,
                    () -> fixture.transactions.findById("missing"));
            StoreFailure inventoryFailure = assertThrows(StoreFailure.class,
                    () -> fixture.inventory.findLowStock(null));

            assertInstanceOf(SQLException.class, catalogFailure.getCause());
            assertEquals("Failed to load catalog", catalogFailure.getMessage());
            assertInstanceOf(SQLException.class, transactionFailure.getCause());
            assertEquals("Failed to read transaction", transactionFailure.getMessage());
            assertInstanceOf(SQLException.class, inventoryFailure.getCause());
            assertEquals("Failed to read low-stock inventory", inventoryFailure.getMessage());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final String database;
        private final Connection admin;
        private final ConnectionPool pool;
        private final JdbcCatalogStore catalog;
        private final JdbcTransactionStore transactions;
        private final JdbcInventoryStore inventory;

        static Fixture open() throws Exception {
            try {
                return new Fixture();
            } catch (SQLException unavailable) {
                throw new TestAbortedException("MySQL unavailable for adapter isolation test", unavailable);
            }
        }

        private Fixture() throws Exception {
            String url = System.getProperty("DB_URL",
                    "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
            String user = System.getProperty("DB_USER", "root");
            String password = System.getProperty("DB_PASS", "");
            database = "layered_adapters_" + UUID.randomUUID().toString().replace("-", "");
            admin = DriverManager.getConnection(url, user, password);
            try (Statement statement = admin.createStatement()) {
                statement.executeUpdate("CREATE DATABASE " + database);
                for (String table : List.of("catalog_item", "inventory", "transaction", "transaction_line")) {
                    statement.executeUpdate("CREATE TABLE " + database + ".`" + table
                            + "` LIKE `" + table + "`");
                }
                statement.executeUpdate("INSERT INTO " + database + ".catalog_item VALUES "
                        + "('B','Bread',2.50),('A','Apple',1.25)");
                statement.executeUpdate("INSERT INTO " + database + ".inventory VALUES "
                        + "('A',1,2),('B',4,3)");
            }
            URI uri = URI.create(url.substring("jdbc:".length()));
            pool = new ConnectionPool(uri.getHost(), uri.getPort() < 0 ? 3306 : uri.getPort(),
                    database, user, password, 1);
            catalog = new JdbcCatalogStore(pool);
            transactions = new JdbcTransactionStore(pool);
            inventory = new JdbcInventoryStore(pool);
        }

        void execute(String sql) throws SQLException {
            try (Statement statement = admin.createStatement()) {
                statement.executeUpdate(sql);
            }
        }

        @Override
        public void close() throws Exception {
            Connection pooled = pool.borrow();
            pooled.close();
            try (admin; Statement statement = admin.createStatement()) {
                statement.executeUpdate("DROP DATABASE " + database);
            }
        }
    }
}
