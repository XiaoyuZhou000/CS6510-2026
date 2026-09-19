package persistence;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public final class InventoryDao {

    public record LowStockRow(String sku, String name, int currentStock, int threshold) {}

    private final ConnectionPool pool;

    public InventoryDao(ConnectionPool pool) {
        this.pool = pool;
    }

    /** Reads current committed stock; overrides never modify the stored per-SKU thresholds. */
    public List<LowStockRow> listLowStock(Integer thresholdOverride) throws SQLException {
        String threshold = thresholdOverride == null ? "i.low_stock_threshold" : "?";
        String sql = "SELECT i.sku, c.name, i.stock_quantity, " + threshold + " AS threshold "
            + "FROM inventory i JOIN catalog_item c ON c.sku = i.sku "
            + "WHERE i.stock_quantity <= " + threshold + " ORDER BY i.sku";
        Connection conn = pool.borrow();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (thresholdOverride != null) {
                ps.setInt(1, thresholdOverride);
                ps.setInt(2, thresholdOverride);
            }
            List<LowStockRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new LowStockRow(rs.getString("sku"), rs.getString("name"),
                        rs.getInt("stock_quantity"), rs.getInt("threshold")));
                }
            }
            return rows;
        } finally {
            pool.release(conn);
        }
    }

    /**
     * Within an already-open (autoCommit=false) connection, decrements stock by quantity
     * only if stock_quantity >= quantity (non-negative guarantee, FR-008, research.md §1).
     *
     * Returns true if the decrement succeeded (1 row affected), false if insufficient stock.
     */
    public boolean decrementIfAvailable(Connection conn, String sku, int quantity)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE inventory SET stock_quantity = stock_quantity - ? " +
                "WHERE sku = ? AND stock_quantity >= ?")) {
            ps.setInt(1, quantity);
            ps.setString(2, sku);
            ps.setInt(3, quantity);
            return ps.executeUpdate() > 0;
        }
    }
}
