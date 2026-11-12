package architecture;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 007 FR-004 (T024, Implementation Rule 3): Transaction records what it sold and nothing about who hears of it. No
 * source under {@code server/transaction/src/} mentions subscribers, subscriptions, or recipients, names the
 * {@code cs6510_notification} schema, or imports the {@code notification} package.
 */
class TransactionHasNoRecipientLogicTest {
    private static final List<String> FORBIDDEN = List.of("subscri", "recipient", "cs6510_notification");
    private static final Pattern NOTIFICATION_IMPORT = Pattern.compile("(?m)^import\\s+(?:static\\s+)?notification\\.");

    @Test
    void transactionSourcesHaveNoRecipientLogic() throws Exception {
        Path root = serverRoot().resolve("transaction/src");
        assertTrue(Files.isDirectory(root), "Missing " + root);
        List<String> violations = new ArrayList<>();
        int count = 0;
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path source : paths.filter(Files::isRegularFile).toList()) {
                count++;
                String body = Files.readString(source);
                String lower = body.toLowerCase(Locale.ROOT);
                for (String word : FORBIDDEN) {
                    if (lower.contains(word)) violations.add(source + " mentions \"" + word + "\"");
                }
                if (NOTIFICATION_IMPORT.matcher(body).find()) violations.add(source + " imports the notification package");
            }
        }
        assertTrue(count > 0, "No Transaction sources found");
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}
