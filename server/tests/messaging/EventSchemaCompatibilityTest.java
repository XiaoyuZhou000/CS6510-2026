package messaging;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import events.DecodedEvent;
import events.EnvelopeCodec;
import events.EventDecodeException;
import events.EventIds;
import events.EventTypes;
import events.ParkReason;
import events.TopicNames;
import org.junit.jupiter.api.Test;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CatalogItemPublished;
import selfcheckout.events.v1.CatalogSnapshotPublished;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.EventEnvelope;
import selfcheckout.events.v1.RejectionReason;
import selfcheckout.events.v1.RestorationNotNeeded;
import selfcheckout.events.v1.RestorationResult;
import selfcheckout.events.v1.SaleCompleted;
import selfcheckout.events.v1.ScanAccepted;
import selfcheckout.events.v1.SoldLine;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestorationRequested;
import selfcheckout.events.v1.StockRestored;
import selfcheckout.events.v1.SubscriberNotified;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Additive-only evolution of the event schemas (FR-014, D3), by the method of 005's
 * {@code rpc.ProtoCompatibilityTest}. Two baselines were generated once from the first event classes:
 * <ul>
 *   <li>{@code baseline/event-descriptor-summary.txt}: one line per message, field (number, name, type), and
 *       enum value. Every line must still hold, so a removed or renumbered field, a changed field type, a removed
 *       enum value, or a removed message fails; new optional fields with new numbers are new lines and pass.</li>
 *   <li>{@code baseline/event-descriptors.textproto}: the four files' descriptors. Bytes written by the current
 *       classes must decode with a parser built only from these, so an old consumer still reads new events.</li>
 * </ul>
 *
 * <p>Feature 007's two files have their version-1 {@code .proto} sources as baselines
 * ({@code baseline/sale_events.proto} and {@code baseline/notification_events.proto}, copied from the design
 * contracts): every field they declare must keep its number, name, label, and type, and bytes must round-trip
 * between the current classes and parsers built only from those sources.</p>
 *
 * <p>Run with {@code -Dproto.baseline.write=true} only to create a missing baseline.</p>
 */
class EventSchemaCompatibilityTest {
    private static final String SUMMARY = "tests/messaging/baseline/event-descriptor-summary.txt";
    private static final String DESCRIPTORS = "tests/messaging/baseline/event-descriptors.textproto";
    /** 007's version-1 sources (T004). */
    private static final List<String> PROTO_BASELINES = List.of("tests/messaging/baseline/sale_events.proto",
            "tests/messaging/baseline/notification_events.proto");
    private static final Pattern PROTO_PACKAGE = Pattern.compile("(?m)^package\\s+([\\w.]+)\\s*;");
    private static final Pattern PROTO_MESSAGE = Pattern.compile("^message\\s+(\\w+)\\s*\\{$");
    private static final Pattern PROTO_FIELD = Pattern.compile("^(repeated\\s+)?([\\w.]+)\\s+(\\w+)\\s*=\\s*(\\d+)\\s*;$");
    private static final Map<String, DescriptorProtos.FieldDescriptorProto.Type> SCALARS = Map.ofEntries(
            Map.entry("string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING),
            Map.entry("bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES),
            Map.entry("bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL),
            Map.entry("int32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32),
            Map.entry("int64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64),
            Map.entry("uint32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32),
            Map.entry("uint64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64),
            Map.entry("sint32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32),
            Map.entry("sint64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64),
            Map.entry("double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE),
            Map.entry("float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT));

    @Test
    void everyBaselineMessageFieldAndEnumValueIsUnchanged() throws Exception {
        writeBaselinesIfAsked();
        Path baseline = resolve(SUMMARY);
        assertTrue(Files.isRegularFile(baseline), "Missing " + SUMMARY);
        List<String> recorded = Files.readAllLines(baseline, StandardCharsets.UTF_8).stream()
                .filter(line -> !line.isBlank() && !line.startsWith("#")).toList();
        for (Message sample : samples()) {
            assertTrue(recorded.contains("message " + sample.getDescriptorForType().getFullName()),
                    "baseline covers " + sample.getDescriptorForType().getFullName());
        }
        Set<String> now = new LinkedHashSet<>(summary());
        List<String> broken = new ArrayList<>();
        for (String line : recorded) if (!now.contains(line)) broken.add(line);
        assertTrue(broken.isEmpty(), "Incompatible event schema changes (renumbered, renamed, retyped, or removed):"
                + System.lineSeparator() + String.join(System.lineSeparator(), broken));
    }

    @Test
    void theSummaryDetectsEachIncompatibleChange() {
        List<String> current = summary();
        String field = current.stream().filter(line -> line.startsWith("message selfcheckout.events.v1.AttemptId field 2 "))
                .findFirst().orElseThrow();
        assertEquals("message selfcheckout.events.v1.AttemptId field 2 attempt_number INT32", field);
        assertFalse(current.contains(field.replace(" field 2 ", " field 3 ")), "a renumbered field is a different line");
        assertFalse(current.contains(field.replace("INT32", "INT64")), "a retyped field is a different line");
        assertTrue(current.contains("enum selfcheckout.events.v1.RejectionReason value 1 INSUFFICIENT_STOCK"));
        assertTrue(current.contains("message selfcheckout.events.v1.StockDeducted"), "a message has its own line");
    }

    @Test
    void currentBytesDecodeWithParsersBuiltFromTheBaselineDescriptors() throws Exception {
        writeBaselinesIfAsked();
        Path baseline = resolve(DESCRIPTORS);
        assertTrue(Files.isRegularFile(baseline), "Missing " + DESCRIPTORS);
        DescriptorProtos.FileDescriptorSet.Builder set = DescriptorProtos.FileDescriptorSet.newBuilder();
        TextFormat.merge(Files.readString(baseline, StandardCharsets.UTF_8), set);
        Map<String, Descriptors.Descriptor> old = new HashMap<>();
        for (DescriptorProtos.FileDescriptorProto file : set.getFileList()) {
            Descriptors.FileDescriptor built = Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
            for (Descriptors.Descriptor type : built.getMessageTypes()) old.put(type.getFullName(), type);
        }
        List<Message> messages = new ArrayList<>(samples());
        messages.add(EventEnvelope.parseFrom(EnvelopeCodec.encode("e-1", EventTypes.STOCK_DEDUCTED, "tx-1", "tx-1",
                EventTypes.INVENTORY, 42, samples().get(4))));
        for (Message current : messages) {
            Descriptors.Descriptor oldType = old.get(current.getDescriptorForType().getFullName());
            assertNotNull(oldType, "baseline has " + current.getDescriptorForType().getFullName());
            DynamicMessage decoded = DynamicMessage.parseFrom(oldType, current.toByteArray());
            assertBaselineFieldsEqual(current, decoded);
        }
    }

    /**
     * 007's baselines (T019): every message and field of {@code sale_events.proto} and
     * {@code notification_events.proto} as first designed is still declared, with the same number, name, label,
     * and type, so no field was renumbered, renamed, retyped, or removed.
     */
    @Test
    void the007ProtoBaselinesKeepEveryFieldNumberNameAndType() throws Exception {
        Set<String> now = new LinkedHashSet<>(summary(List.of(SaleCompleted.getDescriptor().getFile(),
                SubscriberNotified.getDescriptor().getFile())));
        List<String> broken = new ArrayList<>();
        int fields = 0;
        for (String relative : PROTO_BASELINES) {
            Path baseline = resolve(relative);
            assertTrue(Files.isRegularFile(baseline), "Missing " + relative);
            for (String line : summary(List.of(baselineFile(baseline)))) {
                if (line.contains(" field ")) fields++;
                if (!now.contains(line)) broken.add(relative + ": " + line);
            }
        }
        assertEquals(15, fields, "SoldLine (2), SaleCompleted (6), and SubscriberNotified (7) fields in the baselines");
        assertTrue(broken.isEmpty(), "Incompatible changes to 007's event schemas:" + System.lineSeparator()
                + String.join(System.lineSeparator(), broken));
    }

    /** The summary of a baseline source changes when a field is renumbered or retyped, so the gate above bites. */
    @Test
    void the007BaselineSummaryDetectsARenumberedOrRetypedField() throws Exception {
        List<String> baseline = summary(List.of(baselineFile(resolve(PROTO_BASELINES.get(0)))));
        String field = "message selfcheckout.events.v1.SaleCompleted field 3 attempt_number INT32";
        assertTrue(baseline.contains(field), "the baseline parser reads " + field);
        assertTrue(baseline.contains("message selfcheckout.events.v1.SaleCompleted field 6 repeated lines "
                + "MESSAGE selfcheckout.events.v1.SoldLine"), "the baseline parser reads repeated message fields");
        List<String> current = summary(List.of(SaleCompleted.getDescriptor().getFile()));
        assertFalse(current.contains(field.replace(" field 3 ", " field 7 ")), "a renumbered field is a different line");
        assertFalse(current.contains(field.replace("INT32", "INT64")), "a retyped field is a different line");
    }

    /**
     * Bytes written by the current {@code SaleCompleted}, {@code SoldLine}, and {@code SubscriberNotified} classes
     * decode with parsers built only from the baseline sources, and bytes written through those parsers decode
     * with the current classes to equal messages.
     */
    @Test
    void the007MessagesRoundTripThroughParsersBuiltFromTheBaselines() throws Exception {
        Map<String, Descriptors.Descriptor> old = new HashMap<>();
        for (String relative : PROTO_BASELINES) {
            for (Descriptors.Descriptor type : baselineFile(resolve(relative)).getMessageTypes()) old.put(type.getFullName(), type);
        }
        for (Message current : List.of(saleSample(), notificationSample(), saleSample().getLines(0))) {
            Descriptors.Descriptor oldType = old.get(current.getDescriptorForType().getFullName());
            assertNotNull(oldType, "baseline has " + current.getDescriptorForType().getFullName());
            DynamicMessage decoded = DynamicMessage.parseFrom(oldType, current.toByteArray());
            assertBaselineFieldsEqual(current, decoded);
            Message reparsed = current.getParserForType().parseFrom(decoded.toByteArray());
            assertEquals(current, reparsed, "round trip of " + oldType.getFullName());
        }
    }

    /**
     * 007's two types (T008): {@code SaleCompleted} is Transaction's, on the sale-completed topic, routed by its
     * transaction ID; {@code SubscriberNotified} is Notification's, on the subscriber's channel (no fixed topic),
     * routed by its subscriber ID. The envelope codec enforces both producers and routing keys.
     */
    @Test
    void the007TypesHaveTheirProducerTopicAndRoutingKey() {
        SaleCompleted sale = saleSample();
        SubscriberNotified notified = notificationSample();
        assertEquals(EventTypes.TRANSACTION, EventTypes.SALE_COMPLETED.producer());
        assertEquals(Optional.of(TopicNames.Topic.SALE_COMPLETED), EventTypes.SALE_COMPLETED.topic());
        assertFalse(EventTypes.SALE_COMPLETED.isChannelType());
        assertEquals("tx-1", EventTypes.SALE_COMPLETED.routingKeyOf(sale));
        assertEquals(EventTypes.NOTIFICATION, EventTypes.SUBSCRIBER_NOTIFIED.producer());
        assertEquals(Optional.empty(), EventTypes.SUBSCRIBER_NOTIFIED.topic());
        assertTrue(EventTypes.SUBSCRIBER_NOTIFIED.isChannelType());
        assertEquals("sub-1", EventTypes.SUBSCRIBER_NOTIFIED.routingKeyOf(notified));
        assertEquals(Optional.of(EventTypes.SALE_COMPLETED), EventTypes.byName("SaleCompleted"));
        assertEquals(Optional.of(EventTypes.SUBSCRIBER_NOTIFIED), EventTypes.byName("SubscriberNotified"));

        String saleEventId = EventIds.saleEventId("tx-1");
        DecodedEvent decodedSale = EnvelopeCodec.decode(EnvelopeCodec.encode(saleEventId, EventTypes.SALE_COMPLETED,
                "tx-1", "tx-1", EventTypes.TRANSACTION, 42, sale), "tx-1");
        assertEquals(sale, decodedSale.payload());
        assertEquals(saleEventId, decodedSale.eventId());
        String notificationId = EventIds.notificationId("tx-1", "SKU-1", "sub-1");
        DecodedEvent decodedNotification = EnvelopeCodec.decode(EnvelopeCodec.encode(notificationId,
                EventTypes.SUBSCRIBER_NOTIFIED, "sub-1", "tx-1", EventTypes.NOTIFICATION, 43, notified), "sub-1");
        assertEquals(notified, decodedNotification.payload());
        assertEquals(EventTypes.NOTIFICATION, decodedNotification.producer());

        assertThrows(IllegalArgumentException.class, () -> EnvelopeCodec.encode(saleEventId, EventTypes.SALE_COMPLETED,
                "tx-1", "tx-1", EventTypes.NOTIFICATION, 42, sale), "only Transaction produces SaleCompleted");
        assertThrows(IllegalArgumentException.class, () -> EnvelopeCodec.encode(notificationId,
                EventTypes.SUBSCRIBER_NOTIFIED, "sub-1", "tx-1", EventTypes.TRANSACTION, 43, notified),
                "only Notification produces SubscriberNotified");
        EventDecodeException misrouted = assertThrows(EventDecodeException.class, () -> EnvelopeCodec.decode(
                EnvelopeCodec.encode(notificationId, EventTypes.SUBSCRIBER_NOTIFIED, "sub-2", "tx-1",
                        EventTypes.NOTIFICATION, 43, notified), "sub-2"));
        assertEquals(ParkReason.PAYLOAD_MISMATCH, misrouted.reason(), "routing_key must equal subscriber_id");
        EventDecodeException wrongSale = assertThrows(EventDecodeException.class, () -> EnvelopeCodec.decode(
                EnvelopeCodec.encode(saleEventId, EventTypes.SALE_COMPLETED, "tx-2", "tx-2", EventTypes.TRANSACTION,
                        42, sale), "tx-2"));
        assertEquals(ParkReason.PAYLOAD_MISMATCH, wrongSale.reason(), "routing_key must equal transaction_id");
    }

    /** The deterministic IDs (T008): UUID version 3 of their names, stable, and distinct per key. */
    @Test
    void the007EventIdsAreStableNameBasedUuids() {
        String sale = EventIds.saleEventId("tx-1");
        assertEquals(UUID.nameUUIDFromBytes("sale:tx-1".getBytes(StandardCharsets.UTF_8)).toString(), sale);
        assertEquals(3, UUID.fromString(sale).version());
        assertEquals(sale, EventIds.saleEventId("tx-1"));
        assertNotEquals(sale, EventIds.saleEventId("tx-2"));
        String notification = EventIds.notificationId("tx-1", "SKU-1", "sub-1");
        assertEquals(UUID.nameUUIDFromBytes("notification:tx-1:SKU-1:sub-1".getBytes(StandardCharsets.UTF_8)).toString(),
                notification);
        assertEquals(3, UUID.fromString(notification).version());
        assertNotEquals(notification, EventIds.notificationId("tx-1", "SKU-1", "sub-2"));
        assertNotEquals(notification, EventIds.notificationId("tx-1", "SKU-2", "sub-1"));
        assertThrows(IllegalArgumentException.class, () -> EventIds.saleEventId(""));
        assertThrows(IllegalArgumentException.class, () -> EventIds.notificationId("tx-1", null, "sub-1"));
    }

    private static SaleCompleted saleSample() {
        return SaleCompleted.newBuilder().setSaleId("tx-1").setTransactionId("tx-1").setAttemptNumber(2)
                .setCompletedAtMs(1_700_000_000_123L).setStationId("station-7")
                .addLines(SoldLine.newBuilder().setSku("SKU-1").setQuantity(3))
                .addLines(SoldLine.newBuilder().setSku("SKU-2").setQuantity(1)).build();
    }

    private static SubscriberNotified notificationSample() {
        return SubscriberNotified.newBuilder().setNotificationId(EventIds.notificationId("tx-1", "SKU-1", "sub-1"))
                .setSubscriberId("sub-1").setSaleId("tx-1").setSku("SKU-1").setQuantity(3)
                .setCompletedAtMs(1_700_000_000_123L).setSubscriptionVersion(17).build();
    }

    /**
     * A descriptor built only from a baseline {@code .proto} source: its package and its top-level messages with
     * scalar or same-package message fields, which is all 007's two files use.
     */
    private static Descriptors.FileDescriptor baselineFile(Path proto) throws Exception {
        String source = Files.readString(proto, StandardCharsets.UTF_8);
        Matcher declared = PROTO_PACKAGE.matcher(source);
        assertTrue(declared.find(), proto + " declares a package");
        String packageName = declared.group(1);
        DescriptorProtos.FileDescriptorProto.Builder file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("baseline/" + proto.getFileName()).setPackage(packageName).setSyntax("proto3");
        DescriptorProtos.DescriptorProto.Builder message = null;
        for (String raw : source.split("\\R")) {
            String line = raw.replaceFirst("//.*$", "").trim();
            if (line.isEmpty() || line.startsWith("syntax") || line.startsWith("package") || line.startsWith("option")) continue;
            Matcher start = PROTO_MESSAGE.matcher(line);
            Matcher field = PROTO_FIELD.matcher(line);
            if (message == null && start.matches()) {
                message = DescriptorProtos.DescriptorProto.newBuilder().setName(start.group(1));
            } else if (message != null && line.equals("}")) {
                file.addMessageType(message);
                message = null;
            } else if (message != null && field.matches()) {
                DescriptorProtos.FieldDescriptorProto.Builder built = DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName(field.group(3)).setNumber(Integer.parseInt(field.group(4)))
                        .setLabel(field.group(1) != null ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                                : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
                DescriptorProtos.FieldDescriptorProto.Type scalar = SCALARS.get(field.group(2));
                if (scalar != null) built.setType(scalar);
                else built.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName("." + packageName + "." + field.group(2));
                message.addField(built);
            } else {
                fail(proto + ": the baseline parser does not understand: " + raw);
            }
        }
        assertNull(message, proto + " ends inside a message");
        return Descriptors.FileDescriptor.buildFrom(file.build(), new Descriptors.FileDescriptor[0]);
    }

    /** Every field the baseline knows has the value the current class wrote. */
    private static void assertBaselineFieldsEqual(Message current, Message decoded) {
        Descriptors.Descriptor currentType = current.getDescriptorForType();
        for (Descriptors.FieldDescriptor oldField : decoded.getDescriptorForType().getFields()) {
            Descriptors.FieldDescriptor newField = currentType.findFieldByNumber(oldField.getNumber());
            assertNotNull(newField, "field " + oldField.getFullName() + " still exists");
            Object expected = current.getField(newField);
            Object actual = decoded.getField(oldField);
            if (oldField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && !oldField.isRepeated()) {
                assertBaselineFieldsEqual((Message) expected, (Message) actual);
            } else if (oldField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                List<?> expectedItems = (List<?>) expected;
                List<?> actualItems = (List<?>) actual;
                assertEquals(expectedItems.size(), actualItems.size(), oldField.getFullName());
                for (int i = 0; i < expectedItems.size(); i++) {
                    assertBaselineFieldsEqual((Message) expectedItems.get(i), (Message) actualItems.get(i));
                }
            } else {
                assertEquals(normalize(expected), normalize(actual), oldField.getFullName());
            }
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof Descriptors.EnumValueDescriptor enumValue) return enumValue.getNumber();
        if (value instanceof List<?> list) return list.stream().map(EventSchemaCompatibilityTest::normalize).toList();
        return value;
    }

    /** One populated message of every payload type, in catalog order. */
    private static List<Message> samples() {
        AttemptId attempt = AttemptId.newBuilder().setTransactionId("tx-1").setAttemptNumber(3).build();
        return List.of(
                CatalogItemPublished.newBuilder().setSku("SKU-1").setName("Apples").setPrice("1.25").setCatalogVersion("v").build(),
                CatalogSnapshotPublished.newBuilder().setCatalogVersion("v").setProductCount(2000).build(),
                CheckoutRequested.newBuilder().setAttempt(attempt)
                        .addLines(CheckoutLine.newBuilder().setSku("SKU-1").setQuantity(2))
                        .addLines(CheckoutLine.newBuilder().setSku("SKU-2").setQuantity(1)).build(),
                StockRestorationRequested.newBuilder().setAttempt(attempt).build(),
                StockDeducted.newBuilder().setAttempt(attempt).build(),
                StockRejected.newBuilder().setAttempt(attempt).setReason(RejectionReason.INVALID_REQUEST).setSku("SKU-9").build(),
                StockDeductionRefused.newBuilder().setAttempt(attempt).build(),
                StockRestored.newBuilder().setAttempt(attempt).build(),
                RestorationNotNeeded.newBuilder().setAttempt(attempt).setResult(RestorationResult.NOTHING_TO_RESTORE).build(),
                ScanAccepted.newBuilder().setTransactionId("tx-1").setScanSequence(4).setSku("SKU-1")
                        .setProductName("Apples").setUnitPrice("1.25").setAcceptedAtMs(99).build());
    }

    private static List<Descriptors.FileDescriptor> files() {
        return List.of(EventEnvelope.getDescriptor().getFile(), CatalogItemPublished.getDescriptor().getFile(),
                CheckoutRequested.getDescriptor().getFile(), ScanAccepted.getDescriptor().getFile());
    }

    /** One line per message, message field, and enum value of the four 006 event files, in descriptor order. */
    static List<String> summary() {
        return summary(files());
    }

    /** One line per message, message field, and enum value of {@code files}, in descriptor order. */
    static List<String> summary(List<Descriptors.FileDescriptor> files) {
        List<String> lines = new ArrayList<>();
        for (Descriptors.FileDescriptor file : files) {
            for (Descriptors.Descriptor message : file.getMessageTypes()) message(lines, message);
            for (Descriptors.EnumDescriptor type : file.getEnumTypes()) enumeration(lines, type);
        }
        return lines;
    }

    private static void message(List<String> lines, Descriptors.Descriptor message) {
        lines.add("message " + message.getFullName());
        for (Descriptors.FieldDescriptor field : message.getFields()) {
            String type = switch (field.getType()) {
                case MESSAGE -> "MESSAGE " + field.getMessageType().getFullName();
                case ENUM -> "ENUM " + field.getEnumType().getFullName();
                default -> field.getType().name();
            };
            lines.add("message " + message.getFullName() + " field " + field.getNumber() + " "
                    + (field.isRepeated() ? "repeated " : "") + field.getName() + " " + type);
        }
        for (Descriptors.Descriptor nested : message.getNestedTypes()) message(lines, nested);
        for (Descriptors.EnumDescriptor nested : message.getEnumTypes()) enumeration(lines, nested);
    }

    private static void enumeration(List<String> lines, Descriptors.EnumDescriptor type) {
        for (Descriptors.EnumValueDescriptor value : type.getValues()) {
            lines.add("enum " + type.getFullName() + " value " + value.getNumber() + " " + value.getName());
        }
    }

    private static void writeBaselinesIfAsked() throws Exception {
        if (!Boolean.getBoolean("proto.baseline.write")) return;
        Path summary = resolve(SUMMARY);
        if (!Files.exists(summary)) {
            List<String> lines = new ArrayList<>();
            lines.add("# Generated once by messaging.EventSchemaCompatibilityTest from the first event classes (T018, FR-014).");
            lines.add("# Never edit or remove a line: changes inside v1 are additive only.");
            lines.addAll(summary());
            Files.createDirectories(summary.getParent());
            Files.write(summary, lines, StandardCharsets.UTF_8);
        }
        Path descriptors = resolve(DESCRIPTORS);
        if (!Files.exists(descriptors)) {
            DescriptorProtos.FileDescriptorSet.Builder set = DescriptorProtos.FileDescriptorSet.newBuilder();
            for (Descriptors.FileDescriptor file : files()) set.addFile(file.toProto());
            String header = "# Generated once by messaging.EventSchemaCompatibilityTest (T018, FR-014): the v1 event\n"
                    + "# descriptors that old consumers were built from. Never edit.\n";
            Files.writeString(descriptors, header + TextFormat.printer().printToString(set.build()), StandardCharsets.UTF_8);
        }
    }

    private static Path resolve(String relative) {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("tests")) ? cwd.resolve(relative) : cwd.resolve("server").resolve(relative);
    }
}
