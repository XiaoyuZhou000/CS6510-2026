package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class JdbcCheckoutCompletionStoreTest {
    @Test
    void commitsDistinctSortedLinesOnceAndDuplicateIsIdempotent() throws Exception {
        try (Fixture fixture = Fixture.open(5, 1)) {
            fixture.insertOpen("tx-1");
            CheckoutCompletionStore.CompletionCommand command = command("tx-1",
                    new CheckoutCompletionStore.CompletionLine("A", 2, new BigDecimal("1.25")),
                    new CheckoutCompletionStore.CompletionLine("B", 1, new BigDecimal("2.50")));

            assertInstanceOf(CheckoutCompletionStore.Completed.class,
                    fixture.store.completeAtomically(command));
            assertEquals("COMPLETED", fixture.status("tx-1"));
            assertEquals(2, fixture.lineRows("tx-1"));
            assertEquals(3, fixture.stock("A"));
            assertEquals(4, fixture.stock("B"));

            assertInstanceOf(CheckoutCompletionStore.NotOpen.class,
                    fixture.store.completeAtomically(command));
            assertEquals(3, fixture.stock("A"));
            assertEquals(2, fixture.lineRows("tx-1"));
        }
    }

    @Test
    void insufficientStockRollsBackStatusLinesAndEveryDecrement() throws Exception {
        try (Fixture fixture = Fixture.open(1, 1)) {
            fixture.insertOpen("tx-rollback");
            CheckoutCompletionStore.CompletionResult result = fixture.store.completeAtomically(
                    command("tx-rollback",
                            new CheckoutCompletionStore.CompletionLine("A", 1, new BigDecimal("1.25")),
                            new CheckoutCompletionStore.CompletionLine("B", 2, new BigDecimal("2.50"))));
            CheckoutCompletionStore.InsufficientStock insufficient =
                    assertInstanceOf(CheckoutCompletionStore.InsufficientStock.class, result);
            assertEquals("B", insufficient.sku());
            assertEquals("OPEN", fixture.status("tx-rollback"));
            assertEquals(0, fixture.lineRows("tx-rollback"));
            assertEquals(1, fixture.stock("A"));
            assertEquals(1, fixture.stock("B"));
        }
    }

    @Test
    void contendingCompletionsCannotBothSellTheLastUnit() throws Exception {
        try (Fixture fixture = Fixture.open(1, 2);
             ExecutorService workers = Executors.newFixedThreadPool(2)) {
            fixture.insertOpen("tx-a");
            fixture.insertOpen("tx-b");
            Callable<CheckoutCompletionStore.CompletionResult> first = () -> fixture.store.completeAtomically(
                    command("tx-a", new CheckoutCompletionStore.CompletionLine(
                            "A", 1, new BigDecimal("1.25"))));
            Callable<CheckoutCompletionStore.CompletionResult> second = () -> fixture.store.completeAtomically(
                    command("tx-b", new CheckoutCompletionStore.CompletionLine(
                            "A", 1, new BigDecimal("1.25"))));
            List<Future<CheckoutCompletionStore.CompletionResult>> results = workers.invokeAll(List.of(first, second));
            long completed = results.stream().map(f -> get(f)).filter(CheckoutCompletionStore.Completed.class::isInstance).count();
            long rejected = results.stream().map(f -> get(f)).filter(CheckoutCompletionStore.InsufficientStock.class::isInstance).count();
            assertEquals(1, completed);
            assertEquals(1, rejected);
            assertEquals(0, fixture.stock("A"));
        }
    }

    private static CheckoutCompletionStore.CompletionCommand command(
            String id, CheckoutCompletionStore.CompletionLine... lines) {
        BigDecimal total = List.of(lines).stream()
                .map(line -> line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())))
                .reduce(BigDecimal.ZERO.setScale(2), BigDecimal::add);
        return new CheckoutCompletionStore.CompletionCommand(id, total, List.of(lines));
    }

    private static CheckoutCompletionStore.CompletionResult get(
            Future<CheckoutCompletionStore.CompletionResult> future) {
        try { return future.get(); } catch (Exception e) { throw new AssertionError(e); }
    }

    private static final class Fixture implements AutoCloseable {
        private final String database;
        private final Connection admin;
        private final ConnectionPool pool;
        private final int poolSize;
        private final JdbcCheckoutCompletionStore store;

        static Fixture open(int stock, int poolSize) throws Exception {
            try {
                return new Fixture(stock, poolSize);
            } catch (SQLException unavailable) {
                throw new TestAbortedException("MySQL unavailable for adapter test", unavailable);
            }
        }

        private Fixture(int stock, int poolSize) throws Exception {
            String url = System.getProperty("DB_URL",
                    "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
            String user = System.getProperty("DB_USER", "root");
            String password = System.getProperty("DB_PASS", "");
            this.database = "layered_checkout_" + UUID.randomUUID().toString().replace("-", "");
            this.admin = DriverManager.getConnection(url, user, password);
            try (Statement statement = admin.createStatement()) {
                statement.executeUpdate("CREATE DATABASE " + database);
                for (String table : List.of("catalog_item", "inventory", "transaction", "transaction_line")) {
                    statement.executeUpdate("CREATE TABLE " + database + ".`" + table + "` LIKE `" + table + "`");
                }
                statement.executeUpdate("INSERT INTO " + database + ".catalog_item VALUES " +
                        "('A','Apple',1.25),('B','Bread',2.50)");
                statement.executeUpdate("INSERT INTO " + database + ".inventory VALUES " +
                        "('A'," + stock + ",1),('B'," + stock + ",1)");
            }
            URI uri = URI.create(url.substring("jdbc:".length()));
            this.poolSize = poolSize;
            this.pool = new ConnectionPool(uri.getHost(), uri.getPort() < 0 ? 3306 : uri.getPort(),
                    database, user, password, poolSize);
            this.store = new JdbcCheckoutCompletionStore(pool);
        }

        void insertOpen(String id) throws SQLException {
            try (PreparedStatement statement = admin.prepareStatement(
                    "INSERT INTO " + database + ".`transaction` " +
                            "(transaction_id,station_id,status,total_amount) VALUES (?,'station','OPEN',0.00)")) {
                statement.setString(1, id);
                statement.executeUpdate();
            }
        }

        int stock(String sku) throws SQLException {
            return integer("SELECT stock_quantity FROM " + database + ".inventory WHERE sku='" + sku + "'");
        }

        int lineRows(String id) throws SQLException {
            return integer("SELECT COUNT(*) FROM " + database + ".transaction_line WHERE transaction_id='" + id + "'");
        }

        String status(String id) throws SQLException {
            try (Statement statement = admin.createStatement();
                 ResultSet results = statement.executeQuery(
                         "SELECT status FROM " + database + ".`transaction` WHERE transaction_id='" + id + "'")) {
                assertTrue(results.next());
                return results.getString(1);
            }
        }

        private int integer(String sql) throws SQLException {
            try (Statement statement = admin.createStatement(); ResultSet results = statement.executeQuery(sql)) {
                assertTrue(results.next());
                return results.getInt(1);
            }
        }

        @Override
        public void close() throws Exception {
            Connection[] connections = new Connection[poolSize];
            for (int i = 0; i < poolSize; i++) connections[i] = pool.borrow();
            for (Connection connection : connections) connection.close();
            try (admin; Statement statement = admin.createStatement()) {
                statement.executeUpdate("DROP DATABASE " + database);
            }
        }
    }
}
