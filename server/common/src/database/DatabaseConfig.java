package database;

import java.util.Map;

/**
 * One service's database settings (R3, R20). Each service reads only its own
 * {@code <PREFIX>_DB_*} variables plus the shared {@code DB_HOST}, {@code DB_PORT}, and
 * {@code DB_SERVER_PREPARED_STATEMENTS}; the former shared {@code DB_NAME}, {@code DB_USER}, and
 * {@code DB_PASSWORD} are never read, so one exported variable cannot give two services one account.
 */
public record DatabaseConfig(String host, int port, String name, String user, String password,
                             int initialPoolSize, int maxPoolSize, boolean serverPreparedStatements) {
    /** Connector/J flags every service uses (R20); {@code useServerPrepStmts} is a CP3 A/B switch. */
    public static final String URL_FLAGS = "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
            + "&useLocalSessionState=true&useLocalTransactionState=true"
            + "&cachePrepStmts=true&prepStmtCacheSize=256&prepStmtCacheSqlLimit=2048"
            + "&rewriteBatchedStatements=true";

    public DatabaseConfig {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("database host is required");
        if (port <= 0 || port > 65_535) throw new IllegalArgumentException("database port is out of range");
        if (name == null || name.isBlank()) throw new IllegalArgumentException("database name is required");
        if (user == null || user.isBlank()) throw new IllegalArgumentException("database user is required");
        if (password == null) password = "";
        if (initialPoolSize <= 0 || maxPoolSize < initialPoolSize) {
            throw new IllegalArgumentException("Require 0 < initial pool size <= maximum pool size");
        }
    }

    public static DatabaseConfig fromEnvironment(String prefix, String defaultSchema, String defaultUser,
                                                 int defaultInitialPool, int defaultMaxPool) {
        return fromEnvironment(System.getenv(), prefix, defaultSchema, defaultUser, defaultInitialPool,
                defaultMaxPool);
    }

    public static DatabaseConfig fromEnvironment(Map<String, String> environment, String prefix,
                                                 String defaultSchema, String defaultUser,
                                                 int defaultInitialPool, int defaultMaxPool) {
        if (prefix == null || !prefix.matches("[A-Z][A-Z0-9_]*")) {
            throw new IllegalArgumentException("prefix must be an upper-case variable prefix: " + prefix);
        }
        return new DatabaseConfig(
                value(environment, "DB_HOST", "127.0.0.1"),
                Integer.parseInt(value(environment, "DB_PORT", "3307")),
                value(environment, prefix + "_DB_NAME", defaultSchema),
                value(environment, prefix + "_DB_USER", defaultUser),
                environment.getOrDefault(prefix + "_DB_PASSWORD", ""),
                Integer.parseInt(value(environment, prefix + "_DB_POOL_SIZE", Integer.toString(defaultInitialPool))),
                Integer.parseInt(value(environment, prefix + "_DB_POOL_MAX_SIZE", Integer.toString(defaultMaxPool))),
                Boolean.parseBoolean(value(environment, "DB_SERVER_PREPARED_STATEMENTS", "false")));
    }

    public String jdbcUrl() {
        return "jdbc:mysql://" + host + ":" + port + "/" + name + "?" + URL_FLAGS
                + (serverPreparedStatements ? "&useServerPrepStmts=true" : "");
    }

    /** Never prints the password. */
    @Override
    public String toString() {
        return "DatabaseConfig[" + user + "@" + host + ":" + port + "/" + name + ", pool "
                + initialPoolSize + ".." + maxPoolSize + ", serverPreparedStatements=" + serverPreparedStatements + "]";
    }

    private static String value(Map<String, String> environment, String key, String fallback) {
        String value = environment.get(key);
        return value == null || value.isEmpty() ? fallback : value.trim();
    }
}
