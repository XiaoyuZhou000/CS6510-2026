package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConnectionPool implements AutoCloseable {
    private final BlockingQueue<Connection> pool;
    private final List<Connection> connections = new CopyOnWriteArrayList<>();
    private final String url;
    private final String user;
    private final String password;
    private final int maxSize;
    private final AtomicInteger reserved = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger waiters = new AtomicInteger();

    public ConnectionPool(String host, int port, String dbName, String user, String password, int size)
            throws SQLException {
        this(host, port, dbName, user, password, size, size);
    }

    public ConnectionPool(String host, int port, String dbName, String user, String password,
                          int initialSize, int maxSize) throws SQLException {
        if (initialSize <= 0 || maxSize < initialSize)
            throw new IllegalArgumentException("Require 0 < initial pool size <= maximum pool size");
        this.maxSize = maxSize;
        this.user = user;
        this.password = password;
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        pool = new ArrayBlockingQueue<>(maxSize);
        try {
            for (int i = 0; i < initialSize; i++) {
                Connection conn = DriverManager.getConnection(url, user, password);
                connections.add(conn);
                pool.add(conn);
                reserved.incrementAndGet();
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
        waiters.incrementAndGet();
        try {
            while (!closed.get()) {
                Connection conn = pool.poll();
                if (conn == null) conn = grow();
                if (conn == null) conn = pool.poll(100, TimeUnit.MILLISECONDS);
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
        } finally {
            waiters.decrementAndGet();
        }
    }

    private Connection grow() {
        while (!closed.get()) {
            int current = reserved.get();
            if (current >= maxSize) return null;
            if (!reserved.compareAndSet(current, current + 1)) continue;
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                connections.add(conn);
                if (closed.get()) {
                    conn.close();
                    return null;
                }
                return conn;
            } catch (SQLException e) {
                reserved.decrementAndGet();
                throw new StoreFailure("Failed to grow DB connection pool", e);
            }
        }
        return null;
    }

    public Occupancy occupancy() {
        int idle = pool.size();
        return new Occupancy(connections.size(), connections.size() - idle, idle, waiters.get());
    }

    public record Occupancy(int size, int active, int idle, int waiters) { }

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
