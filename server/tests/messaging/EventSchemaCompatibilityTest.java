package messaging;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import events.EnvelopeCodec;
import events.EventTypes;
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
import selfcheckout.events.v1.ScanAccepted;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestorationRequested;
import selfcheckout.events.v1.StockRestored;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * <p>Run with {@code -Dproto.baseline.write=true} only to create a missing baseline.</p>
 */
class EventSchemaCompatibilityTest {
    private static final String SUMMARY = "tests/messaging/baseline/event-descriptor-summary.txt";
    private static final String DESCRIPTORS = "tests/messaging/baseline/event-descriptors.textproto";

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

    /** One line per message, message field, and enum value of the four event files, in descriptor order. */
    static List<String> summary() {
        List<String> lines = new ArrayList<>();
        for (Descriptors.FileDescriptor file : files()) {
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
