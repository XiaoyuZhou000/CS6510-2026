package rpc;

import com.google.protobuf.Descriptors;
import contracts.analytics.v1.AnalyticsIngestProto;
import contracts.inventory.v1.InventoryServiceProto;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Additive-only evolution of the internal contracts (FR-013; contracts/grpc-internal-contracts.md
 * "Compatibility"). {@code baseline/descriptor-summary.txt} was generated once from the first
 * {@code contracts.jar}; every message field must keep its number, name, and type, every enum value its
 * number, and every RPC its request and response types. New lines are allowed.
 *
 * <p>Run with {@code -Dproto.baseline.write=true} only to create a missing baseline.</p>
 */
class ProtoCompatibilityTest {
    private static final String BASELINE = "tests/rpc/baseline/descriptor-summary.txt";

    @Test
    void everyBaselineFieldEnumValueAndRpcIsUnchanged() throws Exception {
        Path baseline = baseline();
        List<String> current = summary();
        if (Boolean.getBoolean("proto.baseline.write") && !Files.exists(baseline)) {
            Files.createDirectories(baseline.getParent());
            Files.write(baseline, current, StandardCharsets.UTF_8);
        }
        assertTrue(Files.isRegularFile(baseline), "Missing " + BASELINE);
        List<String> recorded = Files.readAllLines(baseline, StandardCharsets.UTF_8).stream()
                .filter(line -> !line.isBlank() && !line.startsWith("#")).toList();
        assertTrue(recorded.stream().anyMatch(line -> line.startsWith("rpc selfcheckout.inventory.v1.InventoryService/")));
        assertTrue(recorded.stream().anyMatch(line -> line.startsWith("rpc selfcheckout.analytics.v1.AnalyticsIngestService/")));

        Set<String> now = new LinkedHashSet<>(current);
        List<String> broken = new ArrayList<>();
        for (String line : recorded) if (!now.contains(line)) broken.add(line);
        assertTrue(broken.isEmpty(), "Incompatible contract changes (renumbered, renamed, retyped, or removed):"
                + System.lineSeparator() + String.join(System.lineSeparator(), broken));
    }

    @Test
    void theSummaryDetectsARenumberedField() {
        List<String> current = summary();
        String field = current.stream().filter(line -> line.contains("CheckoutAttemptKey field 2 ")).findFirst().orElseThrow();
        assertFalse(current.contains(field.replace(" field 2 ", " field 3 ")), "a renumbered field is a different line");
        assertTrue(field.endsWith("attempt_number UINT32"), field);
    }

    /** One line per message field, enum value, and RPC of both contract files, in descriptor order. */
    static List<String> summary() {
        List<String> lines = new ArrayList<>();
        for (Descriptors.FileDescriptor file : List.of(InventoryServiceProto.getDescriptor(),
                AnalyticsIngestProto.getDescriptor())) {
            for (Descriptors.Descriptor message : file.getMessageTypes()) message(lines, message);
            for (Descriptors.EnumDescriptor type : file.getEnumTypes()) enumeration(lines, type);
            for (Descriptors.ServiceDescriptor service : file.getServices()) {
                for (Descriptors.MethodDescriptor method : service.getMethods()) {
                    lines.add("rpc " + service.getFullName() + "/" + method.getName() + " "
                            + method.getInputType().getFullName() + " -> " + method.getOutputType().getFullName());
                }
            }
        }
        return lines;
    }

    private static void message(List<String> lines, Descriptors.Descriptor message) {
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

    private static Path baseline() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        Path direct = cwd.resolve(BASELINE);
        return Files.isDirectory(cwd.resolve("tests")) ? direct : cwd.resolve("server").resolve(BASELINE);
    }
}
