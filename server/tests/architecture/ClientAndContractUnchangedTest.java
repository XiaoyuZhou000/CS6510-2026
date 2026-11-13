package architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 007 US6 (T106, FR-031, SC-008): the client interface is unchanged since 006's final commit {@value #BASELINE}. Every
 * file under {@code load-client/src/} and {@code spec/self-checkout-openapi.yaml} is identical to its blob at that
 * commit, compared by git object ID (so line-ending conversion of the checkout does not matter), with no file added or
 * removed; {@code deploy/nginx.conf} differs from it only by the Notification upstream and its {@code /notifications/}
 * location. Aborts when git or the commit is unavailable.
 */
class ClientAndContractUnchangedTest {
    static final String BASELINE = "4a91686";
    private static final String UPSTREAM = """
                upstream notification_service {
                    server 127.0.0.1:8084;
                    keepalive 32;
                }
            """;
    private static final String LOCATION = """
                    location ^~ /notifications/ {
                        proxy_pass http://notification_service;
                    }
            """;

    @Test
    void theLoadClientSourcesAreTheBaselineFiles() throws Exception {
        Path root = repositoryRoot();
        requireBaseline(root);
        TreeSet<String> baseline = new TreeSet<>(lines(git(root, "ls-tree", "-r", "--name-only", BASELINE, "--", "load-client/src")));
        assertFalse(baseline.isEmpty(), "the baseline has load-client sources");
        TreeSet<String> current = new TreeSet<>();
        try (Stream<Path> files = Files.walk(root.resolve("load-client/src"))) {
            files.filter(Files::isRegularFile).forEach(file -> current.add(root.relativize(file).toString().replace('\\', '/')));
        }
        assertEquals(baseline, current, "no load-client source added or removed");
        List<String> changed = new ArrayList<>();
        for (String file : current) {
            if (!sameBlob(root, file)) changed.add(file);
        }
        assertEquals(List.of(), changed, "load-client sources changed since " + BASELINE);
    }

    @Test
    void theOpenApiDocumentIsTheBaselineFile() throws Exception {
        Path root = repositoryRoot();
        requireBaseline(root);
        assertTrue(sameBlob(root, "spec/self-checkout-openapi.yaml"), "spec/self-checkout-openapi.yaml changed since " + BASELINE);
    }

    @Test
    void theGatewayConfigurationOnlyAddsTheNotificationUpstreamAndLocation() throws Exception {
        Path root = repositoryRoot();
        requireBaseline(root);
        String before = normalized(git(root, "show", BASELINE + ":deploy/nginx.conf"));
        String now = normalized(Files.readString(root.resolve("deploy/nginx.conf"), StandardCharsets.UTF_8));
        assertEquals(1, occurrences(now, UPSTREAM), "the notification_service upstream, once");
        assertEquals(1, occurrences(now, LOCATION), "the /notifications/ location, once");
        assertEquals(before, now.replace(UPSTREAM, "").replace(LOCATION, ""),
                "deploy/nginx.conf differs from " + BASELINE + " only by the Notification upstream and location");
    }

    /** True when the working-tree file, cleaned as {@code git add} would clean it, has the baseline's object ID. */
    private static boolean sameBlob(Path root, String file) throws Exception {
        String baseline = git(root, "rev-parse", BASELINE + ":" + file);
        String current = git(root, "hash-object", "--", file);
        return baseline != null && current != null && baseline.trim().equals(current.trim());
    }

    private static void requireBaseline(Path root) throws Exception {
        String type = git(root, "cat-file", "-t", BASELINE);
        assumeTrue(type != null && type.trim().equals("commit"), "git and commit " + BASELINE + " are available");
    }

    private static String normalized(String text) {
        return text.replace("\r\n", "\n");
    }

    private static int occurrences(String text, String part) {
        int count = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + 1)) count++;
        return count;
    }

    private static List<String> lines(String output) {
        if (output == null) return List.of();
        return output.lines().map(String::trim).filter(line -> !line.isEmpty()).toList();
    }

    /** The command's standard output, or null when git is unavailable or fails. */
    private static String git(Path root, String... arguments) throws InterruptedException {
        List<String> command = new ArrayList<>(List.of("git", "-C", root.toString(), "-c", "core.quotepath=off"));
        command.addAll(List.of(arguments));
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(false).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return null;
            }
            return process.exitValue() == 0 ? output : null;
        } catch (IOException missing) {
            return null;
        }
    }

    private static Path repositoryRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd : cwd.getParent();
    }
}
