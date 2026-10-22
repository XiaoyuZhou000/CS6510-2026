package support;

import database.ConnectionPool;
import database.DatabaseConfig;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * A disposable MySQL schema built from one service's {@code db/<service>/schema.sql} (R15).
 *
 * <p>The schema is created with the admin credentials from {@code -DDB_USER} and {@code -DDB_PASS}
 * and is dropped, together with its optional disposable user, on {@link #close()}. It never touches
 * the shared service schemas.</p>
 */
public abstract class TestSchema implements AutoCloseable {
    private static final SecureRandom RANDOM = new SecureRandom();

    private final String name;
    private final String host;
    private final int port;
    private final String adminUser;
    private final String adminPassword;
    private final Connection admin;
    private final String ownUser;
    private final String ownPassword;
    private final Options options;
    private final List<ConnectionPool> pools = new ArrayList<>();
    private ConnectionPool defaultPool;
    private boolean closed;

    /** Options shared by every schema kind. */
    public record Options(boolean ownUser, int initialPool, int maxPool, UnaryOperator<Connection> decorator) {
        public static Options defaults() { return new Options(false, 4, 8, null); }
        public Options withOwnUser() { return new Options(true, initialPool, maxPool, decorator); }
        public Options pool(int initial, int max) { return new Options(ownUser, initial, max, decorator); }
        public Options decorator(UnaryOperator<Connection> wrap) { return new Options(ownUser, initialPool, maxPool, wrap); }
    }

    protected TestSchema(String prefix, String service, Options options) throws Exception {
        this.name = prefix + "_test_" + UUID.randomUUID().toString().replace("-", "");
        this.host = setting("DB_HOST", "127.0.0.1");
        this.port = Integer.parseInt(setting("DB_PORT", "3307"));
        this.adminUser = System.getProperty("DB_USER", System.getenv().getOrDefault("DB_USER", "root"));
        this.adminPassword = System.getProperty("DB_PASS", System.getenv().getOrDefault("DB_PASSWORD", ""));
        this.options = options;
        this.admin = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port
                + "/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&rewriteBatchedStatements=true",
                adminUser, adminPassword);
        boolean created = false;
        String user = null;
        String password = null;
        try (Statement sql = admin.createStatement()) {
            // Fixture DDL that meets a lock leaked by the code under test fails within 30 s instead of hanging.
            sql.execute("SET SESSION lock_wait_timeout = 30");
            sql.executeUpdate("CREATE DATABASE `" + name + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
            created = true;
            admin.setCatalog(name);
            SqlScripts.run(admin, SqlScripts.dbFile(service + "/schema.sql"));
            if (options.ownUser()) {
                user = prefix + "_u_" + HexFormat.of().formatHex(bytes(6));
                password = "Pw" + HexFormat.of().formatHex(bytes(12));
                // A new statement: one created before setCatalog would switch back to no database.
                try (Statement grants = admin.createStatement()) {
                    for (String from : List.of("localhost", "127.0.0.1")) {
                        grants.executeUpdate("CREATE USER '" + user + "'@'" + from + "' IDENTIFIED BY '" + password + "'");
                        grants.executeUpdate("GRANT ALL PRIVILEGES ON `" + name + "`.* TO '" + user + "'@'" + from + "'");
                    }
                }
            }
        } catch (Exception failure) {
            try (Statement sql = admin.createStatement()) {
                if (user != null) dropUser(sql, user);
                if (created) sql.executeUpdate("DROP DATABASE IF EXISTS `" + name + "`");
            } catch (SQLException cleanup) {
                failure.addSuppressed(cleanup);
            }
            admin.close();
            throw failure;
        }
        this.ownUser = user;
        this.ownPassword = password;
    }

    private static String setting(String key, String fallback) {
        String property = System.getProperty(key);
        if (property != null && !property.isBlank()) return property;
        String env = System.getenv(key);
        return env == null || env.isBlank() ? fallback : env;
    }

    private static byte[] bytes(int count) {
        byte[] random = new byte[count];
        RANDOM.nextBytes(random);
        return random;
    }

    public String name() { return name; }
    public String host() { return host; }
    public int port() { return port; }
    /** The disposable user when one was requested, otherwise the admin user. */
    public String user() { return ownUser != null ? ownUser : adminUser; }
    public String password() { return ownUser != null ? ownPassword : adminPassword; }
    public boolean hasOwnUser() { return ownUser != null; }

    /** Autocommit admin connection whose default database is this schema; for fixtures and checks. */
    public Connection admin() { return admin; }

    /** Settings equivalent to the service's {@code DatabaseConfig.fromEnvironment} for this schema. */
    public DatabaseConfig config(int initialPool, int maxPool) {
        return new DatabaseConfig(host, port, name, user(), password(), initialPool, maxPool, false);
    }

    /** The schema's managed pool, built once from the options' sizes and decorator. */
    public synchronized ConnectionPool pool() throws SQLException {
        if (defaultPool == null) {
            defaultPool = newPool(name, options.initialPool(), options.maxPool(), options.decorator());
        }
        return defaultPool;
    }

    /** Another managed pool over this schema; closed with the schema. */
    public synchronized ConnectionPool newPool(String metricsName, int initialPool, int maxPool,
                                               UnaryOperator<Connection> decorator) throws SQLException {
        ConnectionPool pool = new ConnectionPool(config(initialPool, maxPool), metricsName, decorator);
        pools.add(pool);
        return pool;
    }

    /** Runs one statement on the admin connection and returns the update count. */
    public int update(String sql, Object... parameters) throws SQLException {
        try (PreparedStatement statement = admin.prepareStatement(sql)) {
            bind(statement, parameters);
            return statement.executeUpdate();
        }
    }

    /** The first column of the single row of a query, as a long; fails when there is no row. */
    public long queryLong(String sql, Object... parameters) throws SQLException {
        try (PreparedStatement statement = admin.prepareStatement(sql)) {
            bind(statement, parameters);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) throw new IllegalStateException("No row for: " + sql);
                return row.getLong(1);
            }
        }
    }

    /** The first column of the single row of a query, as text, or {@code null}. */
    public String queryString(String sql, Object... parameters) throws SQLException {
        try (PreparedStatement statement = admin.prepareStatement(sql)) {
            bind(statement, parameters);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? row.getString(1) : null;
            }
        }
    }

    protected static void bind(PreparedStatement statement, Object... parameters) throws SQLException {
        for (int i = 0; i < parameters.length; i++) statement.setObject(i + 1, parameters[i]);
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) return;
        closed = true;
        Exception failure = null;
        for (ConnectionPool pool : pools) {
            try { pool.close(); } catch (RuntimeException e) { failure = add(failure, e); }
        }
        try (admin; Statement sql = admin.createStatement()) {
            if (ownUser != null) dropUser(sql, ownUser);
            sql.executeUpdate("DROP DATABASE IF EXISTS `" + name + "`");
        } catch (SQLException e) {
            failure = add(failure, e);
        }
        if (failure != null) throw failure;
    }

    private static void dropUser(Statement sql, String user) throws SQLException {
        for (String from : List.of("localhost", "127.0.0.1")) {
            sql.executeUpdate("DROP USER IF EXISTS '" + user + "'@'" + from + "'");
        }
    }

    private static Exception add(Exception first, Exception next) {
        if (first == null) return next;
        first.addSuppressed(next);
        return first;
    }
}
