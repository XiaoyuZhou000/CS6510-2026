package persistence;

import java.sql.*;

public final class InventoryDao {

    private final ConnectionPool pool;

    public InventoryDao(ConnectionPool pool) {
        this.pool = pool;
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
