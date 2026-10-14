package support;

import database.AcceptedScanStore;
import database.ConnectionPool;
import database.JdbcAcceptedScanStore;
import database.JdbcCheckoutCompletionStore;
import database.JdbcTransactionStore;
import transaction.TransactionService;

import java.net.URI;
import java.sql.*;
import java.util.UUID;

/** Disposable database for durable checkout tests; never resets the shared baseline. */
public final class CheckoutDatabase implements AutoCloseable {
    public static final String SKU = "race-sku";
    public static final String OTHER_SKU = "other-sku";
    private final String name = "checkout_test_" + UUID.randomUUID().toString().replace("-", "");
    private final Connection admin;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private boolean transactionTableUnavailable;
    public final ConnectionPool pool;
    public final TransactionService checkout;

    public CheckoutDatabase(int stock) throws Exception {
        String url = System.getProperty("DB_URL",
            "jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
        user = System.getProperty("DB_USER", System.getenv().getOrDefault("DB_USER", "root"));
        password = System.getProperty("DB_PASS", System.getenv().getOrDefault("DB_PASSWORD", ""));
        URI uri = URI.create(url.substring(5));
        host = uri.getHost();
        port = uri.getPort() < 0 ? 3306 : uri.getPort();
        admin = DriverManager.getConnection(url,user,password);
        try (Statement sql = admin.createStatement()) {
            sql.executeUpdate("CREATE DATABASE " + name);
            for (String table : new String[]{"catalog_item","inventory","transaction","transaction_line","popular_window","popular_item"})
                sql.executeUpdate("CREATE TABLE " + name + ".`" + table + "` LIKE `" + table + "`");
            sql.executeUpdate("CREATE TABLE " + name + ".scan_sequence (sequence_key TINYINT PRIMARY KEY,last_position BIGINT NOT NULL)");
            sql.executeUpdate("INSERT INTO " + name + ".scan_sequence VALUES (1,0)");
            sql.executeUpdate("CREATE TABLE " + name + ".analytics_checkpoint (checkpoint_key TINYINT PRIMARY KEY,last_window_end BIGINT NOT NULL)");
            sql.executeUpdate("INSERT INTO " + name + ".analytics_checkpoint VALUES (1,0)");
            sql.executeUpdate("CREATE TABLE " + name + ".accepted_scan (position BIGINT PRIMARY KEY,transaction_id VARCHAR(40),sku VARCHAR(20),unit_price DECIMAL(10,2),accepted_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))");
            sql.executeUpdate("INSERT INTO " + name + ".catalog_item VALUES ('" + SKU + "','Race Item',2.50),('" + OTHER_SKU + "','Other Item',1.25)");
            sql.executeUpdate("INSERT INTO " + name + ".inventory VALUES ('" + SKU + "'," + stock + ",1),('" + OTHER_SKU + "'," + stock + ",1)");
        }
        pool = new ConnectionPool(host,port,name,user,password,10);
        AcceptedScanStore scans = new JdbcAcceptedScanStore(pool);
        checkout = new TransactionService(new JdbcTransactionStore(pool),scans,
            new JdbcCheckoutCompletionStore(pool));
    }

    /** Starts one independent domain artifact against this disposable database. */
    public Process startService(String domain, int servicePort) throws Exception {
        if (!java.util.Set.of("transaction", "inventory", "analytics").contains(domain))
            throw new IllegalArgumentException("Unknown domain: " + domain);
        java.nio.file.Path root = java.nio.file.Files.isRegularFile(java.nio.file.Path.of("out/transaction.jar"))
            ? java.nio.file.Path.of(".") : java.nio.file.Path.of("server");
        String cp = String.join(java.io.File.pathSeparator,
            root.resolve("out/common.jar").toAbsolutePath().toString(),
            root.resolve("out/" + domain + ".jar").toAbsolutePath().toString(),
            root.resolve("lib/mysql-connector-j-9.0.0.jar").toAbsolutePath().toString());
        String main = domain + "." + Character.toUpperCase(domain.charAt(0))
            + domain.substring(1) + "Main";
        ProcessBuilder builder = new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"),
            "bin", "java").toString(), "-cp", cp, main, Integer.toString(servicePort));
        builder.environment().put("DB_HOST", host);
        builder.environment().put("DB_PORT", Integer.toString(port));
        builder.environment().put("DB_NAME", name);
        builder.environment().put("DB_USER", user);
        builder.environment().put("DB_PASSWORD", password);
        builder.environment().put("DB_POOL_SIZE", "4");
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return builder.start();
    }

    /** Simulates a database schema outage without touching the shared baseline or losing data. */
    public void setTransactionTableUnavailable(boolean unavailable) throws Exception {
        if (transactionTableUnavailable == unavailable) return;
        try (Statement sql = admin.createStatement()) {
            sql.executeUpdate(unavailable
                ? "RENAME TABLE " + name + ".`transaction` TO " + name + ".transaction_outage"
                : "RENAME TABLE " + name + ".transaction_outage TO " + name + ".`transaction`");
        }
        transactionTableUnavailable = unavailable;
    }
    public String databaseName() { return name; }
    public String host() { return host; }
    public int port() { return port; }
    public String user() { return user; }
    public String password() { return password; }

    public int stock() throws Exception { return stock(SKU); }
    public int stock(String sku) throws Exception {
        try (PreparedStatement ps = admin.prepareStatement(
                "SELECT stock_quantity FROM " + name + ".inventory WHERE sku=?")) {
            ps.setString(1,sku);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new IllegalStateException("Missing SKU " + sku);
                return rs.getInt(1);
            }
        }
    }
    public long position() throws Exception {
        try (Statement s=admin.createStatement();ResultSet r=s.executeQuery(
                "SELECT last_position FROM " + name + ".scan_sequence WHERE sequence_key=1")) {
            r.next(); return r.getLong(1);
        }
    }
    public void setStock(int stock) throws Exception {
        try (PreparedStatement ps=admin.prepareStatement(
                "UPDATE " + name + ".inventory SET stock_quantity=? WHERE sku=?")) {
            ps.setInt(1,stock);ps.setString(2,SKU);ps.executeUpdate();
        }
    }
    @Override
    public void close() throws Exception {
        if (transactionTableUnavailable) setTransactionTableUnavailable(false);
        pool.close();
        try (admin;Statement sql=admin.createStatement()) { sql.executeUpdate("DROP DATABASE " + name); }
    }
}
