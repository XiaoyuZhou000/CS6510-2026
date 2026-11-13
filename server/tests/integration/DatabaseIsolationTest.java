package integration;

import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InventoryTestSchema;
import support.LiveSchemas;
import support.NotificationTestSchema;
import support.SqlScripts;
import support.TestSchema;
import support.TransactionTestSchema;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Database isolation (SC-008, R3): a service's credentials reach only its own schema. Each disposable user is
 * granted exactly what {@code db/bootstrap.sql} grants a service user, so every read or change of another
 * service's tables fails with MySQL error 1142, while the owner can reset and fully use its own schema. Every
 * table is probed, so the event tables ({@code outbox_event}, {@code dead_letter_event}, {@code consumer_offset})
 * are covered with the 005 tables (006 US7 scenario 2, SC-011).
 */
class DatabaseIsolationTest {
    private static final int TABLE_ACCESS_DENIED = 1142;
    private static final List<String> SERVICES = List.of("transaction", "inventory", "analytics");

    /** One readable table and one valid change per service, so only the privilege check can fail. */
    private static final Map<String, List<String>> PROBES = Map.of(
            "transaction", List.of("SELECT COUNT(*) FROM %s.`transaction`",
                    "INSERT INTO %s.`transaction` (transaction_id, station_id, started_at) VALUES ('isolation', 's', NOW(3))",
                    "UPDATE %s.`transaction` SET item_count = item_count WHERE transaction_id = 'isolation'",
                    "DELETE FROM %s.`transaction` WHERE transaction_id = 'isolation'"),
            "inventory", List.of("SELECT COUNT(*) FROM %s.inventory",
                    "INSERT INTO %s.catalog_item (sku, name, price) VALUES ('SKU-ISOLATION', 'x', 1.00)",
                    "UPDATE %s.inventory SET stock_quantity = stock_quantity WHERE sku = 'SKU-ISOLATION'",
                    "DELETE FROM %s.catalog_item WHERE sku = 'SKU-ISOLATION'"),
            "analytics", List.of("SELECT COUNT(*) FROM %s.accepted_scan",
                    "INSERT INTO %s.product_name (sku, name) VALUES ('SKU-ISOLATION', 'x')",
                    "UPDATE %s.scan_sequence SET last_position = last_position WHERE sequence_key = 1",
                    "DELETE FROM %s.product_name WHERE sku = 'SKU-ISOLATION'"),
            "notification", List.of("SELECT COUNT(*) FROM %s.subscriber",
                    "INSERT INTO %s.subscriber (subscriber_id, status, channel_topic, channel_ready, created_version, "
                            + "updated_version, created_at, updated_at) VALUES ('isolation', 'ACTIVE', 'x', FALSE, 0, 0, NOW(3), NOW(3))",
                    "UPDATE %s.subscription_clock SET version = version WHERE id = 1",
                    "DELETE FROM %s.subscriber WHERE subscriber_id = 'isolation'"));

    /** 007 (T110): the four service users, Notification's included. */
    private static final List<String> WITH_NOTIFICATION = List.of("transaction", "inventory", "analytics", "notification");
    /**
     * Schema changes, which no service user may make in another schema and the DML-only Notification user may not make
     * in its own (007 T012). Each is harmless if a regression allowed it: an empty table, a drop of a table that does
     * not exist, and a comment on the event table every schema has.
     */
    private static final List<String> DDL = List.of("CREATE TABLE %s.isolation_probe (id INT)",
            "DROP TABLE IF EXISTS %s.isolation_probe_absent",
            "ALTER TABLE %s.`dead_letter_event` COMMENT = 'isolation'");

    /**
     * Every table each schema script creates, including the event tables (006 FR-004, SC-011): a read probe and a
     * delete that would match no row, so only the privilege check can fail and nothing changes even if it does not.
     */
    private static final Map<String, List<String>> TABLES = new LinkedHashMap<>();
    /** The event tables each schema must hold (data-model): outbox, dead letters, and Analytics' offsets. */
    private static final Map<String, List<String>> EVENT_TABLES = Map.of(
            "transaction", List.of("outbox_event", "dead_letter_event"),
            "inventory", List.of("outbox_event", "dead_letter_event"),
            "analytics", List.of("dead_letter_event", "consumer_offset"),
            "notification", List.of("dead_letter_event"));

    private static synchronized List<String> tables(String service) throws Exception {
        if (!TABLES.containsKey(service)) {
            String schema = Files.readString(SqlScripts.dbFile(service + "/schema.sql"), StandardCharsets.UTF_8);
            Matcher table = Pattern.compile("(?i)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([a-z_]+)`?").matcher(schema);
            List<String> found = new ArrayList<>();
            while (table.find()) found.add(table.group(1));
            assertTrue(found.containsAll(EVENT_TABLES.get(service)), service + "/schema.sql creates " + found);
            TABLES.put(service, List.copyOf(found));
        }
        return TABLES.get(service);
    }

    /** {@link #PROBES} plus a read and a no-op delete of every table of {@code service}'s schema. */
    private static List<String> probes(String service) throws Exception {
        List<String> probes = new ArrayList<>(PROBES.get(service));
        for (String table : tables(service)) {
            probes.add("SELECT COUNT(*) FROM %s.`" + table + "`");
            probes.add("DELETE FROM %s.`" + table + "` WHERE 1 = 0");
        }
        return probes;
    }

    @Test
    void bootstrapGrantsEachServiceUserItsOwnSchemaOnly() throws Exception {
        String bootstrap = Files.readString(SqlScripts.dbFile("bootstrap.sql"), StandardCharsets.UTF_8);
        Matcher grant = Pattern.compile("(?i)GRANT\\s+(.+?)\\s+ON\\s+(\\S+)\\s+TO\\s+'([^']+)'@'([^']+)'").matcher(bootstrap);
        Map<String, List<String>> grants = new LinkedHashMap<>();
        while (grant.find()) {
            grants.computeIfAbsent(grant.group(3), user -> new ArrayList<>())
                    .add(grant.group(1).toUpperCase(Locale.ROOT) + " ON " + grant.group(2) + " @" + grant.group(4));
        }
        assertEquals(4, grants.size(), "bootstrap.sql grants exactly the four service users: " + grants);
        assertEquals(List.of("SELECT, INSERT, UPDATE, DELETE ON `cs6510_notification`.* @localhost",
                "SELECT, INSERT, UPDATE, DELETE ON `cs6510_notification`.* @127.0.0.1"), grants.get("cs6510_notification_svc"),
                "the Notification user may only read and write its own rows (007 T012)");
        for (String host : List.of("localhost", "127.0.0.1")) {
            assertTrue(bootstrap.contains("REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'cs6510_notification_svc'@'" + host + "'"),
                    "bootstrap.sql narrows cs6510_notification_svc@" + host + " before granting");
        }
        for (String service : SERVICES) {
            String user = "cs6510_" + service + "_svc";
            assertEquals(List.of("ALL PRIVILEGES ON `cs6510_" + service + "`.* @localhost",
                    "ALL PRIVILEGES ON `cs6510_" + service + "`.* @127.0.0.1"), grants.get(user), user);
            for (String host : List.of("localhost", "127.0.0.1")) {
                assertTrue(bootstrap.contains("REVOKE ALL PRIVILEGES, GRANT OPTION FROM '" + user + "'@'" + host + "'"),
                        "bootstrap.sql narrows " + user + "@" + host + " before granting");
            }
        }
    }

    @Test
    void eachServiceUserIsDeniedEveryOtherSchemaAndOwnsItsOwn() throws Exception {
        TestSchema.Options options = TestSchema.Options.defaults().withOwnUser();
        try (TransactionTestSchema transaction = TransactionTestSchema.create(options);
             InventoryTestSchema inventory = InventoryTestSchema.create(options);
             AnalyticsTestSchema analytics = AnalyticsTestSchema.create(options)) {
            inventory.seed(3, 10);
            transaction.insertOpenTransaction("kept");
            analytics.seedName("SKU-000001", "Item 1");
            Map<String, TestSchema> schemas = Map.of("transaction", transaction, "inventory", inventory,
                    "analytics", analytics);

            for (String owner : SERVICES) {
                assertBootstrapEquivalentGrants(schemas.get(owner));
            }
            Map<String, Long> before = rowCounts(schemas);
            List<String> violations = new ArrayList<>();
            for (String owner : SERVICES) {
                TestSchema own = schemas.get(owner);
                try (Connection connection = connect(own.host(), own.port(), own.name(), own.user(), own.password())) {
                    for (String other : SERVICES) {
                        if (other.equals(owner)) continue;
                        for (String probe : probes(other)) {
                            String sql = String.format(probe, "`" + schemas.get(other).name() + "`");
                            violations.addAll(expectDenied(connection, owner, sql));
                        }
                    }
                }
            }
            assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
            assertEquals(before, rowCounts(schemas), "denied statements changed no other service's data");

            for (String owner : SERVICES) {
                TestSchema own = schemas.get(owner);
                try (Connection connection = connect(own.host(), own.port(), own.name(), own.user(), own.password())) {
                    // The per-service reset runs with only the owner's credentials (R13).
                    SqlScripts.run(connection, SqlScripts.dbFile(owner + "/schema.sql"));
                    if (owner.equals("inventory")) SqlScripts.run(connection, SqlScripts.dbFile("inventory/seed.sql"));
                    for (String probe : probes(owner)) {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(String.format(probe, "`" + own.name() + "`"));
                        }
                    }
                }
            }
            assertEquals(2_000, inventory.queryLong("SELECT COUNT(*) FROM inventory"), "Inventory reseeded itself");
            assertEquals(0, transaction.queryLong("SELECT COUNT(*) FROM `transaction`"), "Transaction reset itself");
            assertEquals(0, analytics.queryLong("SELECT COUNT(*) FROM product_name"), "Analytics reset itself");
        }
    }

    /** The deployed users from {@code db/bootstrap.ps1}, when their passwords are configured. */
    @Test
    void deployedServiceUsersAreDeniedTheOtherServiceSchemas() throws Exception {
        List<String> violations = new ArrayList<>();
        for (String owner : SERVICES) {
            LiveSchemas.Credentials own = LiveSchemas.credentials(owner);
            try (Connection connection = connect(own.host(), own.port(), own.name(), own.user(), own.password())) {
                for (String other : SERVICES) {
                    if (other.equals(owner)) continue;
                    String schema = "`" + LiveSchemas.credentials(other).name() + "`";
                    for (String probe : probes(other)) {
                        violations.addAll(expectDenied(connection, owner, String.format(probe, schema)));
                    }
                }
                try (Statement statement = connection.createStatement();
                     ResultSet row = statement.executeQuery(String.format(PROBES.get(owner).get(0), "`" + own.name() + "`"))) {
                    assertTrue(row.next(), owner + " reads its own schema");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * 007 US7 scenario 2 (T110, FR-035): Notification's user is denied every statement on the other three schemas, each
     * of the other three users is denied every statement on Notification's schema, and Notification's user reads and
     * writes its own rows but cannot change its own schema. Nothing a denied statement touched changed.
     */
    @Test
    void theNotificationUserAndTheOtherThreeAreDeniedEachOthersSchemas() throws Exception {
        TestSchema.Options options = TestSchema.Options.defaults().withOwnUser();
        try (TransactionTestSchema transaction = TransactionTestSchema.create(options);
             InventoryTestSchema inventory = InventoryTestSchema.create(options);
             AnalyticsTestSchema analytics = AnalyticsTestSchema.create(options);
             NotificationTestSchema notification = NotificationTestSchema.create(options)) {
            inventory.seed(3, 10);
            transaction.insertOpenTransaction("kept");
            analytics.seedName("SKU-000001", "Item 1");
            notification.update("INSERT INTO subscriber (subscriber_id, status, channel_topic, channel_ready, "
                    + "created_version, updated_version, created_at, updated_at) "
                    + "VALUES ('kept', 'ACTIVE', 'selfcheckout.notify.kept', TRUE, 1, 1, NOW(3), NOW(3))");
            notification.update("UPDATE subscription_clock SET version = 1 WHERE id = 1");
            Map<String, TestSchema> schemas = Map.of("transaction", transaction, "inventory", inventory,
                    "analytics", analytics, "notification", notification);
            for (String host : List.of("localhost", "127.0.0.1")) {
                assertEquals(List.of("GRANT USAGE ON *.* TO `" + notification.user() + "`@`" + host + "`",
                        "GRANT SELECT, INSERT, UPDATE, DELETE ON `" + notification.name() + "`.* TO `"
                                + notification.user() + "`@`" + host + "`"),
                        grants(notification, host), "the disposable Notification user holds bootstrap's DML-only grant");
            }

            Map<String, Long> before = notificationRowCounts(schemas);
            List<String> violations = new ArrayList<>();
            for (String owner : WITH_NOTIFICATION) {
                TestSchema own = schemas.get(owner);
                try (Connection connection = connect(own.host(), own.port(), own.name(), own.user(), own.password())) {
                    for (String other : WITH_NOTIFICATION) {
                        // Notification against the other three, and the other three against Notification.
                        if (other.equals(owner) || !(owner.equals("notification") || other.equals("notification"))) continue;
                        String schema = "`" + schemas.get(other).name() + "`";
                        List<String> statements = new ArrayList<>(probes(other));
                        statements.addAll(DDL);
                        for (String probe : statements) violations.addAll(expectDenied(connection, owner, String.format(probe, schema)));
                    }
                }
            }
            try (Connection connection = connect(notification.host(), notification.port(), notification.name(),
                    notification.user(), notification.password())) {
                for (String probe : DDL) {
                    violations.addAll(expectDenied(connection, "notification (own schema)",
                            String.format(probe, "`" + notification.name() + "`")));
                }
                for (String probe : probes("notification")) {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(String.format(probe, "`" + notification.name() + "`"));
                    }
                }
            }
            assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
            assertEquals(before, notificationRowCounts(schemas), "denied statements changed no data");
        }
    }

    /** The same in both directions with the deployed users of {@code db/bootstrap.ps1}, when all four passwords are set. */
    @Test
    void theDeployedNotificationUserAndTheOtherThreeAreDeniedEachOthersSchemas() throws Exception {
        Map<String, LiveSchemas.Credentials> credentials = new LinkedHashMap<>();
        for (String service : WITH_NOTIFICATION) credentials.put(service, LiveSchemas.credentials(service));
        List<String> violations = new ArrayList<>();
        for (String owner : WITH_NOTIFICATION) {
            LiveSchemas.Credentials own = credentials.get(owner);
            try (Connection connection = connect(own.host(), own.port(), own.name(), own.user(), own.password())) {
                for (String other : WITH_NOTIFICATION) {
                    if (other.equals(owner) || !(owner.equals("notification") || other.equals("notification"))) continue;
                    String schema = "`" + credentials.get(other).name() + "`";
                    for (String probe : probes(other)) violations.addAll(expectDenied(connection, owner, String.format(probe, schema)));
                }
                try (Statement statement = connection.createStatement();
                     ResultSet row = statement.executeQuery(String.format(PROBES.get(owner).get(0), "`" + own.name() + "`"))) {
                    assertTrue(row.next(), owner + " reads its own schema");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static List<String> grants(TestSchema schema, String host) throws SQLException {
        List<String> grants = new ArrayList<>();
        try (Statement statement = schema.admin().createStatement();
             ResultSet row = statement.executeQuery("SHOW GRANTS FOR '" + schema.user() + "'@'" + host + "'")) {
            while (row.next()) grants.add(row.getString(1));
        }
        return grants;
    }

    private static Map<String, Long> notificationRowCounts(Map<String, TestSchema> schemas) throws SQLException {
        Map<String, Long> counts = rowCounts(schemas);
        TestSchema notification = schemas.get("notification");
        counts.put("subscriber", notification.queryLong("SELECT COUNT(*) FROM subscriber"));
        counts.put("subscription_version", notification.queryLong("SELECT version FROM subscription_clock WHERE id = 1"));
        counts.put("tables", notification.queryLong("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?",
                notification.name()));
        counts.put("dead_letter_comment", notification.queryLong("SELECT COUNT(*) FROM information_schema.tables "
                + "WHERE table_schema = ? AND table_name = 'dead_letter_event' AND table_comment = 'isolation'",
                notification.name()));
        for (String other : SERVICES) {
            counts.put(other + " tables", schemas.get(other).queryLong(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", schemas.get(other).name()));
        }
        return counts;
    }

    /** The disposable user holds exactly bootstrap's grants: usage, plus ALL PRIVILEGES on its own schema. */
    private static void assertBootstrapEquivalentGrants(TestSchema schema) throws SQLException {
        for (String host : List.of("localhost", "127.0.0.1")) {
            List<String> grants = new ArrayList<>();
            try (Statement statement = schema.admin().createStatement();
                 ResultSet row = statement.executeQuery("SHOW GRANTS FOR '" + schema.user() + "'@'" + host + "'")) {
                while (row.next()) grants.add(row.getString(1));
            }
            assertEquals(List.of("GRANT USAGE ON *.* TO `" + schema.user() + "`@`" + host + "`",
                    "GRANT ALL PRIVILEGES ON `" + schema.name() + "`.* TO `" + schema.user() + "`@`" + host + "`"),
                    grants, schema.user() + "@" + host);
        }
    }

    private static List<String> expectDenied(Connection connection, String owner, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            return List.of(owner + " was allowed: " + sql);
        } catch (SQLException denied) {
            return denied.getErrorCode() == TABLE_ACCESS_DENIED ? List.of()
                    : List.of(owner + " got MySQL error " + denied.getErrorCode() + " instead of 1142 for: " + sql);
        }
    }

    private static Map<String, Long> rowCounts(Map<String, TestSchema> schemas) throws SQLException {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("transaction", schemas.get("transaction").queryLong("SELECT COUNT(*) FROM `transaction`"));
        counts.put("catalog_item", schemas.get("inventory").queryLong("SELECT COUNT(*) FROM catalog_item"));
        counts.put("stock", schemas.get("inventory").queryLong("SELECT SUM(stock_quantity) FROM inventory"));
        counts.put("product_name", schemas.get("analytics").queryLong("SELECT COUNT(*) FROM product_name"));
        counts.put("last_position", schemas.get("analytics").queryLong("SELECT last_position FROM scan_sequence"));
        return counts;
    }

    private static Connection connect(String host, int port, String schema, String user, String password)
            throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + schema
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", user, password);
    }
}
