package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ConnectionPool implements AutoCloseable {
    private final BlockingQueue<Connection> pool;
    private final List<Connection> connections = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ConnectionPool(String host, int port, String dbName, String user, String password, int size)
            throws SQLException {
        if (size <= 0) throw new IllegalArgumentException("DB_POOL_SIZE must be positive");
        pool = new ArrayBlockingQueue<>(size);
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        try {
            for (int i = 0; i < size; i++) {
                Connection conn = DriverManager.getConnection(url, user, password);
                connections.add(conn);
                pool.add(conn);
            }
        } catch (SQLException e) {
            for (Connection conn : connections) {
                try { conn.close(); } catch (SQLException cleanup) { e.addSuppressed(cleanup); }
            }
            pool.clear();
            connections.clear();
            throw e;
        }
    }

    /** Waits for a connection, including a prompt exit when this pool shuts down. */
    public Connection borrow() {
        try {
            while (!closed.get()) {
                Connection conn = pool.poll(100, TimeUnit.MILLISECONDS);
                if (conn != null) {
                    if (closed.get()) {
                        conn.close();
                        break;
                    }
                    return conn;
                }
            }
            throw new IllegalStateException("DB connection pool is closed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a DB connection", e);
        } catch (SQLException e) {
            throw new StoreFailure("Failed to close a borrowed DB connection", e);
        }
    }

    /** Returns a connection after use; a shutdown pool closes it instead. */
    public void release(Connection conn) {
        if (conn == null) return;
        if (closed.get()) {
            try { conn.close(); } catch (SQLException e) {
                throw new StoreFailure("Failed to close a returned DB connection", e);
            }
        } else if (!pool.offer(conn)) {
            throw new IllegalStateException("DB connection returned more than once");
        }
    }

    /** Close every connection owned by this JVM, including borrowed ones. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        pool.clear();
        StoreFailure failure = null;
        for (Connection conn : connections) {
            try { conn.close(); } catch (SQLException e) {
                if (failure == null) failure = new StoreFailure("Failed to close DB connection", e);
                else failure.addSuppressed(e);
            }
        }
        if (failure != null) throw failure;
    }
}
