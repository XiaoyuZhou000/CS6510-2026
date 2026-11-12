package architecture;

import events.EventTypes;
import events.TopicNames;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * No publish without an outbox row (FR-010, SC-010, Implementation Rule 2), checked on the built classes: only
 * {@code OutboxDispatcher}, {@code KafkaScanEventSink}, and Notification's {@code NotificationDispatcher} (007,
 * which sends only stored {@code notification} rows) call {@code RecordSender.send}, and only
 * {@code KafkaRecordSender}, the Kafka adapter behind that port, calls a Kafka producer's {@code send}. Every
 * event type has exactly one producer, matching the event catalog (006's and 007's additions), and only its
 * owner service builds its envelope.
 */
class OutboxOnlyPublishTest {
    private static final List<String> ARTIFACTS = List.of("common", "transaction", "inventory", "analytics", "notification");
    private static final Set<String> PRODUCERS = Set.of(EventTypes.TRANSACTION, EventTypes.INVENTORY, EventTypes.NOTIFICATION);
    private static final Set<String> RECORD_SENDS = Set.of("events/RecordSender.send", "messaging/KafkaRecordSender.send");
    private static final Set<String> PRODUCER_SENDS = Set.of("org/apache/kafka/clients/producer/KafkaProducer.send",
            "org/apache/kafka/clients/producer/Producer.send");
    /** The 006 publishers, which must be found by the scan. */
    private static final Set<String> RECORD_SENDERS = Set.of("events/OutboxDispatcher", "messaging/KafkaScanEventSink");
    /** Notification's only publisher (007 Implementation Rule 2); found once US2 adds it. */
    private static final String NOTIFICATION_SENDER = "notification/NotificationDispatcher";
    /**
     * The test-fault wrapper of the dispatcher's channel sender (007 research N13, T059): a {@code RecordSender}
     * decorator that forwards the dispatcher's own sends, so it may call the port it wraps; it originates no record,
     * and only {@code NotificationMain} constructs it, around the channel producer.
     */
    private static final String NOTIFICATION_CHANNEL_WRAPPER = "notification/ChannelFaults";
    private static final Set<String> PRODUCER_CALLERS = Set.of("messaging/KafkaRecordSender");
    private static final Pattern EVENT_TYPE_REFERENCE = Pattern.compile("\\bEventTypes\\.([A-Z_]+)\\b");

    @Test
    void onlyTheDispatcherAndTheScanSinkSendRecords() throws Exception {
        List<String> violations = new ArrayList<>();
        Set<String> recordCallers = new TreeSet<>();
        Set<String> producerCallers = new TreeSet<>();
        boolean notificationSenderBuilt = false;
        int classes = 0;
        for (String artifact : ARTIFACTS) {
            Path jar = serverRoot().resolve("out/" + artifact + ".jar");
            assertTrue(Files.isRegularFile(jar), "Missing artifact " + jar);
            try (JarFile archive = new JarFile(jar.toFile())) {
                for (var entry : archive.stream().filter(e -> e.getName().endsWith(".class")).toList()) {
                    if (entry.getName().equals("module-info.class")) continue;
                    classes++;
                    String className = entry.getName().substring(0, entry.getName().length() - ".class".length());
                    String outer = className.contains("$") ? className.substring(0, className.indexOf('$')) : className;
                    if (outer.equals(NOTIFICATION_SENDER)) notificationSenderBuilt = true;
                    Set<String> calls;
                    try (var in = archive.getInputStream(entry)) {
                        calls = methodReferences(in.readAllBytes());
                    }
                    for (String call : calls) {
                        if (RECORD_SENDS.contains(call)) {
                            recordCallers.add(outer);
                            boolean allowed = RECORD_SENDERS.contains(outer)
                                    || (artifact.equals("notification") && (outer.equals(NOTIFICATION_SENDER)
                                            || outer.equals(NOTIFICATION_CHANNEL_WRAPPER)));
                            if (!allowed) violations.add(className + " calls " + call);
                        }
                        if (PRODUCER_SENDS.contains(call)) {
                            producerCallers.add(outer);
                            if (!PRODUCER_CALLERS.contains(outer)) violations.add(className + " calls " + call);
                        }
                    }
                }
            }
        }
        assertTrue(classes > 0, "No classes found in the service artifacts");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
        Set<String> expectedCallers = new TreeSet<>(RECORD_SENDERS);
        if (notificationSenderBuilt) {
            expectedCallers.add(NOTIFICATION_SENDER);
            expectedCallers.add(NOTIFICATION_CHANNEL_WRAPPER);
            assertChannelWrapperOnlyWrapsTheDispatchersSender(violations);
        }
        assertEquals(expectedCallers, recordCallers, "every outbox publisher is found by the scan");
        assertEquals(PRODUCER_CALLERS, producerCallers, "the Kafka sender adapter is found by the scan");
    }

    /**
     * Each of the twelve types has one producer and one topic, as the event catalog's "Event types" table says
     * together with 007's "Event types (additions)"; a channel-family type's topic is the subscriber's channel.
     */
    @Test
    void everyEventTypeHasExactlyOneProducerAndMatchesTheEventCatalog() throws Exception {
        Map<String, String[]> catalog = catalogEventTypes();
        List<String> violations = new ArrayList<>();
        for (EventTypes type : EventTypes.values()) {
            if (!PRODUCERS.contains(type.producer())) {
                violations.add(type + " has the producer " + type.producer());
            }
            String[] row = catalog.get(type.typeName());
            if (row == null) {
                violations.add(type.typeName() + " is missing from event-catalog.md");
                continue;
            }
            if (!row[1].toLowerCase(Locale.ROOT).startsWith(type.producer())) {
                violations.add(type.typeName() + " is produced by " + type.producer() + " but the catalog says " + row[1]);
            }
            if (type.isChannelType()) {
                if (!row[0].contains("channel")) {
                    violations.add(type.typeName() + " is on the subscriber's channel but the catalog says " + row[0]);
                }
            } else if (!topicMatches(type.topic().orElseThrow(), row[0])) {
                violations.add(type.typeName() + " is on " + type.topic().orElseThrow().baseName()
                        + " but the catalog says " + row[0]);
            }
        }
        Set<String> extra = new TreeSet<>(catalog.keySet());
        for (EventTypes type : EventTypes.values()) extra.remove(type.typeName());
        if (!extra.isEmpty()) violations.add("event-catalog.md lists types EventTypes lacks: " + extra);
        assertEquals(12, EventTypes.values().length, "the catalog's twelve event types");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    /**
     * A production class that builds an envelope ({@code EnvelopeCodec.encode}) lives in the service that owns
     * every type and producer it names; Analytics and the shared code build none.
     */
    @Test
    void onlyTheOwnerServiceBuildsAnEnvelopeForItsTypes() throws Exception {
        Path server = serverRoot();
        List<String> violations = new ArrayList<>();
        Map<String, Integer> builders = new LinkedHashMap<>();
        for (String owner : ARTIFACTS) {
            try (Stream<Path> paths = Files.walk(server.resolve(owner + "/src"))) {
                for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String body = Files.readString(source);
                    if (!body.contains("EnvelopeCodec.encode(")) continue;
                    builders.merge(owner, 1, Integer::sum);
                    if (!PRODUCERS.contains(owner)) {
                        violations.add(source + " builds an envelope outside a producing service");
                        continue;
                    }
                    Matcher reference = EVENT_TYPE_REFERENCE.matcher(body);
                    while (reference.find()) {
                        String name = reference.group(1);
                        String producer = switch (name) {
                            case "TRANSACTION" -> EventTypes.TRANSACTION;
                            case "INVENTORY" -> EventTypes.INVENTORY;
                            case "NOTIFICATION" -> EventTypes.NOTIFICATION;
                            default -> isEventType(name) ? EventTypes.valueOf(name).producer() : null;
                        };
                        if (producer != null && !producer.equals(owner)) {
                            violations.add(source + " builds envelopes but names " + name + ", owned by " + producer);
                        }
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
        assertTrue(builders.containsKey(EventTypes.TRANSACTION), "Transaction builds its envelopes: " + builders);
        assertTrue(builders.containsKey(EventTypes.INVENTORY), "Inventory builds its envelopes: " + builders);
    }

    /** {@code new ChannelFaults(} appears in Notification's production code only in {@code NotificationMain}. */
    private static void assertChannelWrapperOnlyWrapsTheDispatchersSender(List<String> violations) throws IOException {
        int constructions = 0;
        try (Stream<Path> paths = Files.walk(serverRoot().resolve("notification/src"))) {
            for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                if (!Files.readString(source).contains("new ChannelFaults(")) continue;
                if (source.getFileName().toString().equals("NotificationMain.java")) constructions++;
                else violations.add(source + " constructs the channel fault wrapper outside NotificationMain");
            }
        }
        assertEquals(1, constructions, "NotificationMain wraps the channel sender in ChannelFaults");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static boolean isEventType(String name) {
        for (EventTypes type : EventTypes.values()) if (type.name().equals(name)) return true;
        return false;
    }

    /** Catalog column "catalog", "checkout", "outcome", or "scan-accepted" names the topic's base. */
    private static boolean topicMatches(TopicNames.Topic topic, String column) {
        String base = topic.baseName().replaceFirst("^selfcheckout\\.", "").replaceFirst("\\.v\\d+$", "");
        return base.equals(column) || base.endsWith("-" + column);
    }

    /**
     * {@code event_type -> [topic column, producer column]} from 006's "Event types" table and 007's
     * "Event types (additions)" table.
     */
    private static Map<String, String[]> catalogEventTypes() throws IOException {
        Map<String, String[]> rows = new LinkedHashMap<>();
        readEventTypes(serverRoot().getParent().resolve("specs/006-event-based-architecture/contracts/event-catalog.md"),
                "## Event types", rows);
        readEventTypes(serverRoot().getParent().resolve("specs/007-notification-service/contracts/sale-completed-event.md"),
                "## Event types (additions)", rows);
        return rows;
    }

    private static void readEventTypes(Path file, String heading, Map<String, String[]> rows) throws IOException {
        assertTrue(Files.isRegularFile(file), "Missing " + file);
        int before = rows.size();
        boolean inTable = false;
        for (String line : Files.readAllLines(file)) {
            if (line.startsWith("## ")) inTable = line.equals(heading);
            if (!inTable || !line.startsWith("| `")) continue;
            String[] cells = line.split("\\|");
            String type = cells[1].trim().replaceAll("`", "").replace("(reserved)", "").trim();
            assertFalse(rows.containsKey(type), "the event catalogs list " + type + " twice");
            rows.put(type, new String[] {cells[2].trim(), cells[3].trim()});
        }
        assertTrue(rows.size() > before, "No event types found under " + heading + " in " + file);
    }

    /** Every {@code owner.name} of the class file's Methodref and InterfaceMethodref constants (JVMS 4.4). */
    private static Set<String> methodReferences(byte[] classFile) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(classFile));
        if (in.readInt() != 0xCAFEBABE) throw new IOException("not a class file");
        in.readUnsignedShort();
        in.readUnsignedShort();
        int count = in.readUnsignedShort();
        String[] utf8 = new String[count];
        int[] classNames = new int[count];
        int[] nameAndTypeNames = new int[count];
        List<int[]> methodRefs = new ArrayList<>();
        for (int index = 1; index < count; index++) {
            int tag = in.readUnsignedByte();
            switch (tag) {
                case 1 -> utf8[index] = in.readUTF();
                case 3, 4 -> in.readInt();
                case 5, 6 -> {
                    in.readLong();
                    index++;
                }
                case 7 -> classNames[index] = in.readUnsignedShort();
                case 8, 16, 19, 20 -> in.readUnsignedShort();
                case 9, 17, 18 -> in.readInt();
                case 10, 11 -> methodRefs.add(new int[] {in.readUnsignedShort(), in.readUnsignedShort()});
                case 12 -> {
                    nameAndTypeNames[index] = in.readUnsignedShort();
                    in.readUnsignedShort();
                }
                case 15 -> {
                    in.readUnsignedByte();
                    in.readUnsignedShort();
                }
                default -> throw new IOException("unknown constant pool tag " + tag);
            }
        }
        Set<String> references = new HashSet<>();
        for (int[] ref : methodRefs) {
            references.add(utf8[classNames[ref[0]]] + "." + utf8[nameAndTypeNames[ref[1]]]);
        }
        return references;
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}
