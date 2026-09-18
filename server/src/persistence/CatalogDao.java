package persistence;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class CatalogDao {

    public record CatalogRow(String sku, String name, BigDecimal price) {}

    private final ConnectionPool pool;

    public CatalogDao(ConnectionPool pool) {
        this.pool = pool;
    }

    /** Loads every row from catalog_item. Called once at startup to seed CatalogCache. */
    public List<CatalogRow> loadAll() throws SQLException {
        Connection conn = pool.borrow();
        try {
            List<CatalogRow> rows = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT sku, name, price FROM catalog_item")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new CatalogRow(
                            rs.getString("sku"),
                            rs.getString("name"),
                            rs.getBigDecimal("price")
                        ));
                    }
                }
            }
            return rows;
        } finally {
            pool.release(conn);
        }
    }
}
