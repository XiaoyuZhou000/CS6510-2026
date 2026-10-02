package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.AnalyticsDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JdbcPopularWindowStoreTest {
    @Test
    void recoversMaximumEndAndReadsLatestRanksInStoredOrderWithALimit() throws Exception {
        try (AnalyticsDatabase db = open()) {
            assertEquals(0, db.windows.readMaxWindowEnd());
            assertTrue(db.windows.readLatest(10).isEmpty());

            db.windows.writeWindow(snapshot(1, 1000,
                    rank(1, "sku2", 60), rank(2, "sku1", 40)));
            db.windows.writeWindow(snapshot(501, 1500,
                    rank(1, "sku3", 70), rank(2, "sku1", 30)));

            assertEquals(1500, db.windows.readMaxWindowEnd());
            var latest = db.windows.readLatest(1).orElseThrow();
            assertEquals(1500, latest.windowEnd());
            assertEquals(List.of("sku3"), latest.ranks().stream().map(r -> r.sku()).toList());
            assertTrue(db.windows.readLatest(0).orElseThrow().ranks().isEmpty());
        }
    }

    @Test
    void headerAndRanksRollBackTogetherWhenAnyRankFails() throws Exception {
        try (AnalyticsDatabase db = open()) {
            Connection setup = db.pool.borrow();
            try (Statement statement = setup.createStatement()) {
                statement.executeUpdate("ALTER TABLE popular_item ADD CONSTRAINT "
                        + "fk_test_popular_sku FOREIGN KEY (sku) REFERENCES catalog_item(sku)");
            } finally {
                db.pool.release(setup);
            }
            PopularWindowStore.PopularWindowSnapshot invalidForeignKey = snapshot(
                    1, 1000, rank(1, "missing-sku", 1000));

            assertThrows(StoreFailure.class, () -> db.windows.writeWindow(invalidForeignKey));
            assertEquals(0, rowCount(db, "popular_window"));
            assertEquals(0, rowCount(db, "popular_item"));
        }
    }

    @Test
    void equivalentAlreadyCommittedWindowIsRecognizedWithoutDuplicatingItsBoundary() throws Exception {
        try (AnalyticsDatabase db = open()) {
            PopularWindowStore.PopularWindowSnapshot window = snapshot(
                    1, 1000, rank(1, "sku2", 60), rank(2, "sku1", 40));

            db.windows.writeWindow(window);
            assertDoesNotThrow(() -> db.windows.writeWindow(window));

            assertEquals(1, rowCount(db, "popular_window"));
            assertEquals(2, rowCount(db, "popular_item"));
            assertThrows(StoreFailure.class, () -> db.windows.writeWindow(snapshot(
                    1, 1000, rank(1, "sku1", 1000))));
            assertEquals(1, rowCount(db, "popular_window"));
        }
    }

    private static AnalyticsDatabase open() throws Exception {
        try {
            return new AnalyticsDatabase();
        } catch (SQLException unavailable) {
            throw new TestAbortedException("MySQL unavailable for adapter test", unavailable);
        }
    }

    private static PopularWindowStore.PopularWindowSnapshot snapshot(
            long start, long end, PopularWindowStore.SnapshotRank... ranks) {
        return new PopularWindowStore.PopularWindowSnapshot(start, end, List.of(ranks));
    }

    private static PopularWindowStore.SnapshotRank rank(int rank, String sku, long count) {
        return new PopularWindowStore.SnapshotRank(rank, sku, count);
    }

    private static int rowCount(AnalyticsDatabase db, String table) throws SQLException {
        Connection connection = db.pool.borrow();
        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(results.next());
            return results.getInt(1);
        } finally {
            db.pool.release(connection);
        }
    }
}
