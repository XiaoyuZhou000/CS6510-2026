package support;

import events.RecordStream;
import events.TopicNames;
import messaging.KafkaClients;
import messaging.KafkaRecordSender;
import messaging.KafkaRecordStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assumptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Live-broker helpers for {@code tests/messaging} (D14).
 *
 * <ul>
 *   <li>The broker is {@code KAFKA_TEST_BOOTSTRAP} (default {@code 127.0.0.1:19092}). Without it a test is
 *       skipped, or fails when {@code STRICT_LIVE_GATES=1}.</li>
 *   <li>Every run uses its own topic prefix, and topics are created through the {@code Admin} client with the
 *       catalog's partitions and configuration. A topic is never deleted: on the Windows broker a deletion
 *       takes the broker down (D1). Prefixed topics accumulate until the next baseline reset.</li>
 *   <li>With {@code KAFKA_TEST_BROKER_CONTROL=1}, restart tests stop and start the native broker through
 *       {@code deploy/kafka/stop-broker.ps1} and {@code start-broker.ps1 -NoTopics}, which need {@code KAFKA_HOME}.
 *       On Windows every stop is a forced kill, so a restart and a kill are the same case.</li>
 * </ul>
 */
public final class KafkaTestSupport {
    public static final String DEFAULT_BOOTSTRAP = "127.0.0.1:19092";
    private static final String NO_ROLL_BYTES = "1073741824";
    private static final String NO_ROLL_MS = Long.toString(Long.MAX_VALUE);

    private KafkaTestSupport() { }

    public static String bootstrap() {
        return setting("KAFKA_TEST_BOOTSTRAP", DEFAULT_BOOTSTRAP);
    }

    /** Environment for {@link KafkaClients}, pointing at the test broker. */
    public static Map<String, String> clientEnvironment() {
        return Map.of(KafkaClients.BOOTSTRAP_VARIABLE, bootstrap());
    }

    /** Skips the calling test without a reachable broker, or fails it when {@code STRICT_LIVE_GATES=1}. */
    public static void requireBroker() {
        String problem = null;
        try (Admin admin = admin()) {
            admin.describeCluster().nodes().get(5, TimeUnit.SECONDS);
        } catch (Exception unreachable) {
            problem = "Kafka broker unavailable at " + bootstrap() + ": " + unreachable;
        }
        skipOrFail(problem);
    }

    /** Skips, or fails under {@code STRICT_LIVE_GATES=1}, when {@code problem} is not null. */
    public static void skipOrFail(String problem) {
        if (problem == null) return;
        if ("1".equals(setting("STRICT_LIVE_GATES", ""))) fail(problem + " (STRICT_LIVE_GATES=1)");
        Assumptions.abort(problem);
    }

    /** A fresh prefix such as {@code t-1a2b3c4d.}, so a run never meets another run's records or offsets. */
    public static TopicNames runTopics() {
        return new TopicNames("t-" + UUID.randomUUID().toString().substring(0, 8) + ".");
    }

    /** Creates the four catalog topics under {@code names}' prefix with their production configuration. */
    public static void createCatalogTopics(TopicNames names) {
        List<NewTopic> topics = new ArrayList<>();
        for (TopicNames.Topic topic : TopicNames.Topic.values()) {
            topics.add(newTopic(names.name(topic), topic.partitions(), topic == TopicNames.Topic.CATALOG ? "compact" : "delete",
                    topic == TopicNames.Topic.CATALOG ? "-1" : "604800000"));
        }
        create(topics);
    }

    /** Creates one topic with the never-rolling segment settings of the Windows broker. */
    public static void createTopic(String name, int partitions, String cleanupPolicy) {
        create(List.of(newTopic(name, partitions, cleanupPolicy, "-1")));
    }

    public static Admin admin() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        return Admin.create(properties);
    }

    public static KafkaRecordSender sender(String clientId, int lingerMs) {
        return new KafkaRecordSender(KafkaClients.producer(clientEnvironment(), clientId, lingerMs));
    }

    /** Streams for a {@code ConsumerLoop}; {@code group} is null for a reader that never commits. */
    public static Supplier<RecordStream> streams(String clientId, String group) {
        return () -> new KafkaRecordStream(KafkaClients.consumer(clientEnvironment(), clientId + "-" + UUID.randomUUID(),
                group), group, clientId);
    }

    /**
     * Waits until {@code group} is stable with exactly {@code members} members and every partition of
     * {@code topic} assigned, so a test's first event is consumed without waiting for a join, and a crashed
     * member's partitions have moved to the live ones; fails after {@code timeout}.
     */
    public static void awaitGroupAssigned(String group, String topic, int partitions, int members, Duration timeout) {
        awaitGroupAssigned(group, topic, partitions, members, java.util.Set.of(), timeout);
    }

    /**
     * Waits until {@code group} has committed every record of {@code topic}, so a process restarted with a fault
     * hook never meets a redelivery of an event its predecessor already handled; fails after {@code timeout}.
     */
    public static void awaitNoLag(String group, String topic, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String last = "not read";
        try (Admin admin = admin()) {
            while (System.nanoTime() < deadline) {
                try {
                    var description = admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS).get(topic);
                    Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> latest =
                            new java.util.HashMap<>();
                    for (var partition : description.partitions()) {
                        latest.put(new org.apache.kafka.common.TopicPartition(topic, partition.partition()),
                                org.apache.kafka.clients.admin.OffsetSpec.latest());
                    }
                    var ends = admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS);
                    var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                            .get(5, TimeUnit.SECONDS);
                    long lag = 0;
                    for (var end : ends.entrySet()) {
                        var offset = committed.get(end.getKey());
                        lag += end.getValue().offset() - (offset == null ? 0 : offset.offset());
                    }
                    last = lag + " records behind";
                    if (lag == 0) return;
                } catch (Exception notYet) {
                    last = notYet.toString();
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        fail("Consumer group " + group + " never caught up on " + topic + ": " + last);
    }

    /** The member IDs of {@code group} now; empty when it has none or cannot be described. */
    public static java.util.Set<String> memberIds(String group) {
        try (Admin admin = admin()) {
            return admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS).get(group).members()
                    .stream().map(member -> member.consumerId()).collect(java.util.stream.Collectors.toSet());
        } catch (Exception unknown) {
            return java.util.Set.of();
        }
    }

    /**
     * As above, and none of the members may be one of {@code stale}: a process killed without shutdown hooks,
     * as Windows stops test processes, stays in its group until its session expires, and a group of only its
     * members looks just like a stable group of the new process.
     */
    public static void awaitGroupAssigned(String group, String topic, int partitions, int members,
                                          java.util.Set<String> stale, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        String last = "not described";
        try (Admin admin = admin()) {
            while (System.nanoTime() < deadline) {
                try {
                    var description = admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS)
                            .get(group);
                    long assigned = description.members().stream()
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .filter(partition -> partition.topic().equals(topic)).count();
                    last = description.groupState() + " with " + description.members().size() + " members and "
                            + assigned + " partitions assigned";
                    boolean fresh = description.members().stream().noneMatch(member -> stale.contains(member.consumerId()));
                    if (description.groupState() == org.apache.kafka.common.GroupState.STABLE && assigned == partitions
                            && description.members().size() == members && fresh) {
                        return;
                    }
                } catch (Exception notYet) {
                    last = notYet.toString();
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        fail("Consumer group " + group + " was not assigned all " + partitions + " partitions of " + topic + ": " + last);
    }

    public static boolean brokerControlEnabled() {
        return "1".equals(setting("KAFKA_TEST_BROKER_CONTROL", ""));
    }

    /** Skips, or fails under {@code STRICT_LIVE_GATES=1}, unless broker restarts are allowed. */
    public static void requireBrokerControl() {
        requireBroker();
        skipOrFail(brokerControlEnabled() ? null : "KAFKA_TEST_BROKER_CONTROL=1 is not set; broker restarts are not allowed");
    }

    public static void stopBroker() {
        runBrokerScript("stop-broker.ps1");
    }

    /** Starts the broker without touching topics and waits until it answers. */
    public static void startBroker() {
        runBrokerScript("start-broker.ps1", "-NoTopics");
    }

    public static void restartBroker() {
        stopBroker();
        startBroker();
    }

    private static void runBrokerScript(String script, String... arguments) {
        Path path = repositoryRoot().resolve("deploy").resolve("kafka").resolve(script);
        List<String> command = new ArrayList<>(List.of("powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", path.toString()));
        command.addAll(List.of(arguments));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        String kafkaHome = setting("KAFKA_HOME", "");
        if (!kafkaHome.isEmpty()) builder.environment().put("KAFKA_HOME", kafkaHome);
        try {
            Path log = Files.createTempFile("kafka-" + script, ".log");
            Process process = builder.redirectOutput(log.toFile()).start();
            if (!process.waitFor(180, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                fail(script + " did not finish within 180 s; see " + log);
            }
            if (process.exitValue() != 0) fail(script + " failed: " + Files.readString(log));
        } catch (Exception failure) {
            fail(script + " could not run: " + failure);
        }
    }

    private static NewTopic newTopic(String name, int partitions, String cleanupPolicy, String retentionMs) {
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put("cleanup.policy", cleanupPolicy);
        configs.put("retention.ms", retentionMs);
        configs.put("segment.bytes", NO_ROLL_BYTES);
        configs.put("segment.ms", NO_ROLL_MS);
        return new NewTopic(name, partitions, (short) 1).configs(configs);
    }

    private static void create(List<NewTopic> topics) {
        try (Admin admin = admin()) {
            for (var created : admin.createTopics(topics).values().entrySet()) {
                try {
                    created.getValue().get(30, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException failure) {
                    // Two in-process services of one run create the same prefixed topics; the second is a no-op.
                    if (!(failure.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw failure;
                }
            }
        } catch (Exception failure) {
            fail("Creating test topics failed: " + failure);
        }
        // The partitions' leaders are elected asynchronously; wait until every one is visible.
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        try (Admin admin = admin()) {
            while (true) {
                try {
                    boolean ready = admin.describeTopics(topics.stream().map(NewTopic::name).toList()).allTopicNames()
                            .get(5, TimeUnit.SECONDS).values().stream()
                            .allMatch(description -> description.partitions().stream().allMatch(p -> p.leader() != null));
                    if (ready) return;
                } catch (Exception notYet) {
                    // Metadata is still propagating.
                }
                if (System.nanoTime() > deadline) fail("Test topics have no leaders after 30 s");
                Thread.sleep(100);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static String setting(String name, String fallback) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static Path repositoryRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("deploy")) ? cwd : cwd.getParent();
    }
}
