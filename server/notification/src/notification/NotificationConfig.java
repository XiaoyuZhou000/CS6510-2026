package notification;

import database.DatabaseConfig;
import events.TopicNames;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Notification Service's settings (007 contracts/deployment-and-evidence.md "Notification configuration").
 * Every value comes from the environment, with the contract's default when the variable is unset or blank:
 *
 * <ul>
 *   <li>{@code NOTIFICATION_HTTP_PORT} 8084;</li>
 *   <li>{@code NOTIFICATION_DB_URL} {@code jdbc:mysql://127.0.0.1:3307/cs6510_notification} with 006's flags
 *       (only its host, port, and schema are read; the flags are always {@link DatabaseConfig#URL_FLAGS}),
 *       {@code NOTIFICATION_DB_USER} {@code cs6510_notification_svc}, {@code NOTIFICATION_DB_PASSWORD}, and
 *       {@code NOTIFICATION_DB_POOL_SIZE} 12 and {@code NOTIFICATION_DB_POOL_MAX_SIZE} 20;</li>
 *   <li>{@code KAFKA_TOPIC_PREFIX}, as in 006 ({@code KAFKA_BOOTSTRAP} is read by {@code NotificationMain}
 *       through {@code KafkaClients}, because only the composition root may reference the messaging layer);</li>
 *   <li>{@code NOTIFICATION_SALE_CONSUMER_THREADS} 4, {@code NOTIFICATION_LANE_IN_FLIGHT} 64,
 *       {@code NOTIFICATION_PARK_AFTER_CHARGED} 8, and {@code NOTIFICATION_SWEEP_MS} 1000;</li>
 *   <li>{@code NOTIFICATION_TEST_FAULTS} unset; {@code 1} enables {@code POST /notifications/ops/faults}.</li>
 * </ul>
 *
 * <p>{@link #toString()} never includes the password.</p>
 */
public record NotificationConfig(int httpPort, DatabaseConfig database, TopicNames topics,
                                 int saleConsumerThreads, int laneInFlight, int parkAfterCharged, long sweepMs,
                                 boolean testFaults) {
    public static final String PREFIX = "NOTIFICATION";
    public static final String DEFAULT_SCHEMA = "cs6510_notification";
    public static final String DEFAULT_USER = "cs6510_notification_svc";
    public static final int DEFAULT_HTTP_PORT = 8084;
    public static final int DEFAULT_POOL_SIZE = 12;
    public static final int DEFAULT_POOL_MAX_SIZE = 20;
    public static final int DEFAULT_SALE_CONSUMER_THREADS = 4;
    public static final int DEFAULT_LANE_IN_FLIGHT = 64;
    public static final int DEFAULT_PARK_AFTER_CHARGED = 8;
    public static final long DEFAULT_SWEEP_MS = 1000;
    private static final Pattern MYSQL_URL = Pattern.compile("^jdbc:mysql://([^:/?]+)(?::(\\d+))?/([^?/]+)(?:\\?.*)?$");

    public NotificationConfig {
        if (httpPort <= 0 || httpPort > 65_535) throw new IllegalArgumentException("NOTIFICATION_HTTP_PORT is out of range");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(topics, "topics");
        positive(saleConsumerThreads, "NOTIFICATION_SALE_CONSUMER_THREADS");
        positive(laneInFlight, "NOTIFICATION_LANE_IN_FLIGHT");
        positive(parkAfterCharged, "NOTIFICATION_PARK_AFTER_CHARGED");
        if (sweepMs <= 0) throw new IllegalArgumentException("NOTIFICATION_SWEEP_MS must be positive");
    }

    public static NotificationConfig fromEnvironment(Map<String, String> environment) {
        Map<String, String> settings = environment;
        String url = value(environment, PREFIX + "_DB_URL", null);
        if (url != null) {
            Matcher parts = MYSQL_URL.matcher(url);
            if (!parts.matches()) {
                throw new IllegalArgumentException("NOTIFICATION_DB_URL must look like jdbc:mysql://host:port/schema");
            }
            Map<String, String> overridden = new HashMap<>(environment);
            overridden.put("DB_HOST", parts.group(1));
            if (parts.group(2) != null) overridden.put("DB_PORT", parts.group(2));
            overridden.put(PREFIX + "_DB_NAME", parts.group(3));
            settings = overridden;
        }
        DatabaseConfig database = DatabaseConfig.fromEnvironment(settings, PREFIX, DEFAULT_SCHEMA, DEFAULT_USER,
                DEFAULT_POOL_SIZE, DEFAULT_POOL_MAX_SIZE);
        return new NotificationConfig(
                intValue(environment, PREFIX + "_HTTP_PORT", DEFAULT_HTTP_PORT),
                database,
                TopicNames.fromEnvironment(environment),
                intValue(environment, PREFIX + "_SALE_CONSUMER_THREADS", DEFAULT_SALE_CONSUMER_THREADS),
                intValue(environment, PREFIX + "_LANE_IN_FLIGHT", DEFAULT_LANE_IN_FLIGHT),
                intValue(environment, PREFIX + "_PARK_AFTER_CHARGED", DEFAULT_PARK_AFTER_CHARGED),
                Long.parseLong(value(environment, PREFIX + "_SWEEP_MS", Long.toString(DEFAULT_SWEEP_MS))),
                "1".equals(value(environment, PREFIX + "_TEST_FAULTS", "")));
    }

    /** The same settings on another HTTP port, for a port given on the command line. */
    public NotificationConfig withHttpPort(int port) {
        return new NotificationConfig(port, database, topics, saleConsumerThreads, laneInFlight,
                parkAfterCharged, sweepMs, testFaults);
    }

    @Override
    public String toString() {
        return "NotificationConfig[port " + httpPort + ", " + database
                + ", topic prefix '" + topics.prefix() + "', sale consumer threads " + saleConsumerThreads
                + ", lane in-flight " + laneInFlight + ", park after " + parkAfterCharged + " charged failures"
                + ", sweep " + sweepMs + " ms, test faults " + (testFaults ? "on" : "off") + "]";
    }

    private static void positive(int value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
    }

    private static int intValue(Map<String, String> environment, String key, int fallback) {
        return Integer.parseInt(value(environment, key, Integer.toString(fallback)));
    }

    private static String value(Map<String, String> environment, String key, String fallback) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
