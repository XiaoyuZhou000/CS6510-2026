package database;

import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

/**
 * Bounded JDBC pool that grows from its initial size to its maximum on demand.
 *
 * <p>A pool built from a {@link DatabaseConfig} is <em>managed</em> (R20): every physical connection
 * is opened once in manual-commit mode at READ COMMITTED, {@link #release} rolls back any open
 * transaction and restores those settings, and borrow wait, hold time, and occupancy are recorded as
 * {@code <name>.pool.*} metrics (R21). The legacy constructors keep their original behavior for
 * callers that are not migrated yet.</p>
 */
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
    private final AtomicInteger borrowed = new AtomicInteger();
    private final AtomicInteger borrowedHighWater = new AtomicInteger();
    private final boolean managed;
    private final UnaryOperator<Connection> decorator;
    private final ConcurrentHashMap<Connection, Lease> leases = new ConcurrentHashMap<>();
    private final LatencyHistogram borrowWait;
    private final LatencyHistogram holdTime;

    public ConnectionPool(String host, int port, String dbName, String user, String password, int size)
            throws SQLException {
        this(host, port, dbName, user, password, size, size);
    }

    public ConnectionPool(String host, int port, String dbName, String user, String password,
                          int initialSize, int maxSize) throws SQLException {
        this("jdbc:mysql://" + host + ":" + port + "/" + dbName
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                user, password, initialSize, maxSize, false, null, null);
    }

    public ConnectionPool(DatabaseConfig config, String metricsName) throws SQLException {
        this(config, metricsName, null);
    }

    /**
     * Builds a managed pool. {@code decorator} is a test-only hook that wraps each physical
     * connection, for example to count statements; production passes {@code null} (identity). A
     * decorated connection must keep identity {@code equals} and {@code hashCode}.
     */
    public ConnectionPool(DatabaseConfig config, String metricsName, UnaryOperator<Connection> decorator)
            throws SQLException {
        this(config.jdbcUrl(), config.user(), config.password(), config.initialPoolSize(),
                config.maxPoolSize(), true, requireName(metricsName), decorator);
    }

    private ConnectionPool(String url, String user, String password, int initialSize, int maxSize,
                           boolean managed, String metricsName, UnaryOperator<Connection> decorator)
            throws SQLException {
        if (initialSize <= 0 || maxSize < initialSize)
            throw new IllegalArgumentException("Require 0 < initial pool size <= maximum pool size");
        this.maxSize = maxSize;
        this.user = user;
        this.password = password;
        this.url = url;
        this.managed = managed;
        this.decorator = decorator == null ? UnaryOperator.identity() : decorator;
        pool = new ArrayBlockingQueue<>(maxSize);
        try {
            for (int i = 0; i < initialSize; i++) {
                pool.add(open());
                reserved.incrementAndGet();
            }
        } catch (SQLException e) {
            for (Connection conn : connections) {
                try { conn.close(); } catch (SQLException cleanup) { e.addSuppressed(cleanup); }
            }
            pool.clear();
            connections.clear();
            leases.clear();
            throw e;
        }
        if (managed) {
            borrowWait = PerfMetrics.timer(metricsName + ".pool.borrowWait");
            holdTime = PerfMetrics.timer(metricsName + ".pool.holdTime");
            PerfMetrics.gauge(metricsName + ".pool.size", connections::size);
            PerfMetrics.gauge(metricsName + ".pool.occupancy", borrowed::get);
            PerfMetrics.gauge(metricsName + ".pool.occupancyHighWater", borrowedHighWater::get);
            PerfMetrics.gauge(metricsName + ".pool.waiters", waiters::get);
        } else {
            borrowWait = null;
            holdTime = null;
        }
    }

    /** Opens one physical connection; a managed pool configures its session exactly once here. */
    private Connection open() throws SQLException {
        Connection raw = DriverManager.getConnection(url, user, password);
        connections.add(raw);
        if (!managed) return raw;
        try {
            raw.setAutoCommit(false);
            raw.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } catch (SQLException e) {
            connections.remove(raw);
            try { raw.close(); } catch (SQLException cleanup) { e.addSuppressed(cleanup); }
            throw e;
        }
        Connection handed = decorator.apply(raw);
        leases.put(handed, new Lease(raw));
        return handed;
    }

    /** Waits for a connection, including a prompt exit when this pool shuts down. */
    public Connection borrow() {
        long started = System.nanoTime();
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
                    onBorrow(conn, started);
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

    private void onBorrow(Connection conn, long started) {
        int active = borrowed.incrementAndGet();
        if (active > borrowedHighWater.get()) borrowedHighWater.accumulateAndGet(active, Math::max);
        if (!managed) return;
        Lease lease = leases.get(conn);
        lease.out.set(true);
        long now = System.nanoTime();
        lease.borrowedAt = now;
        borrowWait.record(now - started);
    }

    private Connection grow() {
        while (!closed.get()) {
            int current = reserved.get();
            if (current >= maxSize) return null;
            if (!reserved.compareAndSet(current, current + 1)) continue;
            try {
                Connection conn = open();
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

    /** The most connections that were borrowed at the same time. */
    public int activeHighWater() {
        return borrowedHighWater.get();
    }

    public record Occupancy(int size, int active, int idle, int waiters) { }

    /** Returns a connection after use; a shutdown pool closes it instead. */
    public void release(Connection conn) {
        if (conn == null) return;
        Lease lease = null;
        if (managed) {
            lease = leases.get(conn);
            if (lease == null) throw new IllegalArgumentException("DB connection does not belong to this pool");
            if (!lease.out.compareAndSet(true, false)) throw new IllegalStateException("DB connection returned more than once");
            holdTime.record(System.nanoTime() - lease.borrowedAt);
        }
        borrowed.decrementAndGet();
        if (closed.get()) {
            try { conn.close(); } catch (SQLException e) {
                throw new StoreFailure("Failed to close a returned DB connection", e);
            }
            return;
        }
        if (lease != null && !resetSession(lease.raw)) {
            discard(conn, lease.raw);
            return;
        }
        if (!pool.offer(conn)) {
            throw new IllegalStateException("DB connection returned more than once");
        }
    }

    /** Ends any open transaction and restores manual commit at READ COMMITTED; false if broken. */
    private static boolean resetSession(Connection raw) {
        try {
            if (raw.getAutoCommit()) raw.setAutoCommit(false);
            else raw.rollback();
            if (raw.getTransactionIsolation() != Connection.TRANSACTION_READ_COMMITTED) {
                raw.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
            return true;
        } catch (SQLException broken) {
            return false;
        }
    }

    /** Drops a connection that cannot be reused; the pool grows a replacement on demand. */
    private void discard(Connection handed, Connection raw) {
        leases.remove(handed);
        connections.remove(raw);
        reserved.decrementAndGet();
        try { raw.close(); } catch (SQLException ignored) { }
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

    private static String requireName(String metricsName) {
        if (metricsName == null || metricsName.isBlank())
            throw new IllegalArgumentException("A managed pool needs a metrics name");
        return metricsName;
    }

    private static final class Lease {
        final Connection raw;
        final AtomicBoolean out = new AtomicBoolean();
        volatile long borrowedAt;

        Lease(Connection raw) { this.raw = raw; }
    }
}
