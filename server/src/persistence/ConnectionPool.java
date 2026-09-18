package persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class ConnectionPool {

    private final BlockingQueue<Connection> pool;

    public ConnectionPool(String host, int port, String dbName, String user, String password, int size)
            throws SQLException {
        if (size <= 0) {
            throw new IllegalArgumentException("DB_POOL_SIZE must be positive");
        }
        pool = new ArrayBlockingQueue<>(size);
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        try {
            for (int i = 0; i < size; i++) {
                Connection conn = DriverManager.getConnection(url, user, password);
                pool.add(conn);
            }
        } catch (SQLException e) {
            for (Connection conn : pool) {
                try { conn.close(); } catch (SQLException cleanup) { e.addSuppressed(cleanup); }
            }
            pool.clear();
            throw e;
        }
    }

    /** Blocks until a connection is available. */
    public Connection borrow() {
        try {
            return pool.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a DB connection", e);
        }
    }

    /** Returns a connection to the pool. Must be called after every borrow(). */
    public void release(Connection conn) {
        if (conn != null) {
            pool.add(conn);
        }
    }
}
