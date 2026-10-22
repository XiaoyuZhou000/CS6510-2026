package integration;

import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InventoryTestSchema;
import support.LiveSchemas;
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
 * service's tables fails with MySQL error 1142, while the owner can reset and fully use its own schema.
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
                    "DELETE FROM %s.product_name WHERE sku = 'SKU-ISOLATION'"));

    @Test
    void bootstrapGrantsEachServiceUserItsOwnSchemaOnly() throws Exception {
        String bootstrap = Files.readString(SqlScripts.dbFile("bootstrap.sql"), StandardCharsets.UTF_8);
        Matcher grant = Pattern.compile("(?i)GRANT\\s+(.+?)\\s+ON\\s+(\\S+)\\s+TO\\s+'([^']+)'@'([^']+)'").matcher(bootstrap);
        Map<String, List<String>> grants = new LinkedHashMap<>();
        while (grant.find()) {
            grants.computeIfAbsent(grant.group(3), user -> new ArrayList<>())
                    .add(grant.group(1).toUpperCase(Locale.ROOT) + " ON " + grant.group(2) + " @" + grant.group(4));
        }
        assertEquals(3, grants.size(), "bootstrap.sql grants exactly the three service users: " + grants);
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
                        for (String probe : PROBES.get(other)) {
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
                    for (String probe : PROBES.get(owner)) {
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
                    for (String probe : PROBES.get(other)) {
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
