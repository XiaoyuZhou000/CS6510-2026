package architecture;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 007 FR-002 (T024, contracts/sale-completed-event.md "Production rules"): {@code SaleCompleted} is created only by
 * T3. In Transaction, {@code SaleEvents} and {@code EventTypes.SALE_COMPLETED} are named only by {@code SaleEvents}
 * itself, the outcome handler that runs T3, the store that commits it, and the composition root that wires them.
 * That no other service builds the envelope is {@code OutboxOnlyPublishTest}'s owner rule.
 */
class SaleEventOnlyFromT3Test {
    private static final Pattern REFERENCE = Pattern.compile("\\bSaleEvents\\b|\\bSALE_COMPLETED\\b");
    private static final Set<String> ALLOWED = Set.of("CheckoutOutcomeHandler.java", "JdbcCheckoutAttemptStore.java",
            "SaleEvents.java", "TransactionMain.java");

    @Test
    void onlyT3SourcesInTransactionReferenceTheSaleEvent() throws Exception {
        Set<String> referencing = new TreeSet<>();
        List<String> violations = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(serverRoot().resolve("transaction/src"))) {
            for (Path source : paths.filter(path -> path.toString().endsWith(".java")).toList()) {
                if (!REFERENCE.matcher(Files.readString(source)).find()) continue;
                String name = source.getFileName().toString();
                referencing.add(name);
                if (!ALLOWED.contains(name)) violations.add(source + " references SaleEvents or SALE_COMPLETED outside T3");
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
        assertTrue(referencing.containsAll(Set.of("CheckoutOutcomeHandler.java", "SaleEvents.java", "TransactionMain.java")),
                "the builder, the T3 handler, and the composition root all take part: " + referencing);
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}
