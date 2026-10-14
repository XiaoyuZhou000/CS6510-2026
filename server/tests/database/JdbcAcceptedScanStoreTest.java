package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class JdbcAcceptedScanStoreTest {
    private static final String URL = System.getProperty("DB_URL",
        "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    private static final String USER = System.getProperty("DB_USER", "root");
    private static final String PASS = System.getProperty("DB_PASS", "");

    @Test
    void eachSuccessfulScanCommitsLineTotalAndUnpositionedEventTogether() throws Exception {
        try (Fixture db = Fixture.open()) {
            String id = db.createTransaction();
            long before = db.eventCount();
            var scan = db.scans.scanAtomically(id, "SKU-000001");
            assertInstanceOf(AcceptedScanStore.Accepted.class, scan);
            assertEquals(before + 1, db.eventCount());
            assertEquals(1, db.integer("SELECT COUNT(*) FROM accepted_scan WHERE transaction_id='" + id + "' AND position IS NULL"));
            assertEquals(1, db.integer("SELECT quantity FROM transaction_line WHERE transaction_id='" + id + "'"));
            assertEquals(1, db.integer("SELECT SUM(quantity) FROM transaction_line WHERE transaction_id='" + id + "'"));
            assertEquals(db.money("SELECT price FROM catalog_item WHERE sku='SKU-000001'"),
                db.money("SELECT total_amount FROM transaction WHERE transaction_id='" + id + "'"));
        }
    }

    @Test
    void invalidSkuAndSqlFailureLeaveNoPartialScan() throws Exception {
        try (Fixture db = Fixture.open()) {
            String id = db.createTransaction();
            long before = db.eventCount();
            assertInstanceOf(AcceptedScanStore.UnknownSku.class,
                db.scans.scanAtomically(id, "DOES-NOT-EXIST"));
            assertEquals(before, db.eventCount());
            assertEquals(0, db.integer("SELECT COUNT(*) FROM transaction_line WHERE transaction_id='" + id + "'"));
            assertEquals(0, db.integer("SELECT COUNT(*) FROM accepted_scan WHERE transaction_id='" + id + "'"));
            try (Statement ddl = db.admin.createStatement()) { ddl.executeUpdate("DROP TABLE accepted_scan"); }
            assertThrows(StoreFailure.class, () -> db.scans.scanAtomically(id, "SKU-000001"));
            assertEquals(0, db.integer("SELECT COUNT(*) FROM transaction_line WHERE transaction_id='" + id + "'"));
            assertEquals(BigDecimal.ZERO.setScale(2),
                db.money("SELECT total_amount FROM transaction WHERE transaction_id='" + id + "'"));
        }
    }

    @Test
    void concurrentScansCommitDistinctUnpositionedEvents() throws Exception {
        try (Fixture db = Fixture.open(); ExecutorService workers = Executors.newFixedThreadPool(4)) {
            String id = db.createTransaction();
            String other = db.createTransaction();
            long before = db.eventCount();
            List<Callable<AcceptedScanStore.ScanResult>> calls = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                String target = i % 2 == 0 ? id : other;
                calls.add(() -> db.scans.scanAtomically(target, "SKU-000001"));
            }
            for (Future<AcceptedScanStore.ScanResult> result : workers.invokeAll(calls))
                assertInstanceOf(AcceptedScanStore.Accepted.class, result.get());
            assertEquals(before + 8, db.eventCount());
            assertEquals(8, db.integer("SELECT COUNT(*) FROM accepted_scan"));
            assertEquals(4, db.integer("SELECT quantity FROM transaction_line WHERE transaction_id='" + id + "'"));
            assertEquals(4, db.integer("SELECT quantity FROM transaction_line WHERE transaction_id='" + other + "'"));
            assertEquals(8, db.integer("SELECT COUNT(DISTINCT event_id) FROM accepted_scan"));
            assertEquals(8, db.integer("SELECT COUNT(*) FROM accepted_scan WHERE position IS NULL"));
        }
    }

    private static final class Fixture implements AutoCloseable {
        final Connection admin;
        final String database;
        final ConnectionPool pool;
        final JdbcAcceptedScanStore scans;
        static Fixture open() throws Exception {
            try { return new Fixture(); }
            catch (SQLException unavailable) { throw new TestAbortedException("MySQL unavailable", unavailable); }
        }
        Fixture() throws Exception {
            admin = DriverManager.getConnection(URL, USER, PASS);
            database = "phase3_scan_" + UUID.randomUUID().toString().replace("-", "");
            try (Statement ddl = admin.createStatement()) {
                ddl.executeUpdate("CREATE DATABASE " + database);
                for (String table : List.of("catalog_item","inventory","transaction","transaction_line"))
                    ddl.executeUpdate("CREATE TABLE " + database + ".`" + table + "` LIKE cs6510_selfcheckout.`" + table + "`");
                ddl.executeUpdate("CREATE TABLE " + database + ".scan_sequence (sequence_key TINYINT PRIMARY KEY,last_position BIGINT NOT NULL)");
                ddl.executeUpdate("INSERT INTO " + database + ".scan_sequence VALUES (1,0)");
                ddl.executeUpdate("CREATE TABLE " + database + ".accepted_scan (event_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,position BIGINT NULL UNIQUE KEY,transaction_id VARCHAR(40),sku VARCHAR(20),unit_price DECIMAL(10,2),accepted_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),KEY idx_scan_pending(position,event_id))");
                ddl.executeUpdate("INSERT INTO " + database + ".catalog_item VALUES ('SKU-000001','Item 1',0.85)");
            }
            admin.setCatalog(database);
            var uri = java.net.URI.create(URL.substring(5));
            pool = new ConnectionPool(uri.getHost(), uri.getPort() < 0 ? 3306 : uri.getPort(),
                database, USER, PASS, 4);
            scans = new JdbcAcceptedScanStore(pool);
        }
        String createTransaction() {
            String id = UUID.randomUUID().toString();
            new JdbcTransactionStore(pool).insertOpen(id, "phase3-test");
            return id;
        }
        long eventCount() throws SQLException {
            try (Statement s = admin.createStatement(); ResultSet r =
                    s.executeQuery("SELECT COUNT(*) FROM accepted_scan")) {
                r.next(); return r.getLong(1);
            }
        }
        int integer(String sql) throws SQLException {
            try (Statement s = admin.createStatement(); ResultSet r = s.executeQuery(sql)) {
                r.next(); return r.getInt(1);
            }
        }
        BigDecimal money(String sql) throws SQLException {
            try (Statement s = admin.createStatement(); ResultSet r = s.executeQuery(sql)) {
                r.next(); return r.getBigDecimal(1);
            }
        }
        public void close() throws Exception {
            pool.close();
            admin.setCatalog("cs6510_selfcheckout");
            try (Statement ddl = admin.createStatement()) { ddl.executeUpdate("DROP DATABASE " + database); }
            admin.close();
        }
    }
}







