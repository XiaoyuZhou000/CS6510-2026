package architecture;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelineArchitectureTest {
    @Test
    void pipelineHasThreeExplicitQueueConnectedFiltersAndNamedWorkers() throws Exception {
        String service = read("analytics/AnalyticsService.java");
        String window = read("analytics/WindowFilter.java");
        String ranking = read("analytics/RankingFilter.java");
        String persistence = read("analytics/PersistenceFilter.java");

        assertTrue(service.contains("new LinkedBlockingQueue<>()"));
        assertTrue(service.contains("analytics-window"));
        assertTrue(service.contains("analytics-ranking"));
        assertTrue(service.contains("analytics-persistence"));
        assertTrue(window.contains("BlockingQueue<PipelineMessage.IngressMessage>"));
        assertTrue(window.contains("BlockingQueue<PipelineMessage.WindowMessage>"));
        assertTrue(ranking.contains("BlockingQueue<PipelineMessage.WindowMessage>"));
        assertTrue(ranking.contains("BlockingQueue<PipelineMessage.RankingMessage>"));
        assertTrue(persistence.contains("BlockingQueue<PipelineMessage.RankingMessage>"));
        assertFalse(window.contains("RankingFilter"));
        assertFalse(ranking.contains("PersistenceFilter"));
    }

    @Test
    void filtersHaveNoHttpJdbcOrCrossLayerImplementationDependencies() throws Exception {
        for (String file : new String[]{"WindowFilter.java", "RankingFilter.java", "PersistenceFilter.java"}) {
            String source = read("analytics/" + file);
            assertFalse(source.contains("com.sun.net.httpserver"), file);
            assertFalse(source.contains("java.sql"), file);
            assertFalse(source.contains("Jdbc"), file);
            assertFalse(source.contains("api."), file);
            assertFalse(source.contains("transaction."), file);
        }
        assertFalse(read("analytics/RankingFilter.java").contains("PopularWindowStore"));
    }

    @Test
    void externalContractSchemaAndLoadClientSourcesRemainByteForByteCompatible() throws Exception {
        Map<String, String> expectedSha256 = Map.ofEntries(
                Map.entry("spec/self-checkout-openapi.yaml", "3272ac80b3fdb63f127cd47ed8bf42b03d9d8efccc0d12cbeafb54f2d6819ff5"),
                Map.entry("db/init.sql", "942f60dccb3f25d973649c65730f1d2f42706e3c059bf5cd1590112180ee6032"),
                Map.entry("load-client/src/ApiClient.java", "90cc36bf53c95a0507ac6e41ee47967377d538919c8f62fcd7b0681e0f5b5167"),
                Map.entry("load-client/src/CatalogItem.java", "980c468443a2f9d6731d537a1c271631410b872a1d518ba9fdd775567fe567bb"),
                Map.entry("load-client/src/Config.java", "ac530593a275db7214f4d1feb026e131ffbec1e860d340884726bc192091fd48"),
                Map.entry("load-client/src/ItemSampler.java", "c0dda8a7256fa94ac68c4c12b49649eec1957f6601a5a21c73ed9f500a96efab"),
                Map.entry("load-client/src/Json.java", "3a27c61ef0cedc3fb296d9e2c120ed31f5493b91258860e3220f47fdf77a5904"),
                Map.entry("load-client/src/Main.java", "480af01167c69a61b42c7b41ded3d71704000fbfa514ae43d77ab0a6fe3e62a8"),
                Map.entry("load-client/src/Metrics.java", "40b8617a7bbed6be7ddb1ae221110a59ddbd10dafdfdb466b35519cb8d550ee8"),
                Map.entry("load-client/src/ReportWriter.java", "1901500add1d9b86b13ef720465e3797f1da8d9bb6bc68c5ce844ee9ca1b31cd"),
                Map.entry("load-client/src/StationWorker.java", "79f1d7b2ad98d7047d5bec19fb0e7678886559e64f1b59c2e449bcf8d037843c"));

        Path root = projectRoot();
        List<String> actualClientSources;
        try (var files = Files.walk(root.resolve("load-client/src"))) {
            actualClientSources = files.filter(Files::isRegularFile)
                    .map(root::relativize)
                    .map(path -> path.toString().replace('\\', '/'))
                    .sorted()
                    .toList();
        }
        assertEquals(expectedSha256.keySet().stream()
                        .filter(path -> path.startsWith("load-client/src/"))
                        .sorted().toList(),
                actualClientSources,
                "The immutable load-client source set changed");

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (Map.Entry<String, String> file : expectedSha256.entrySet()) {
            String actual = HexFormat.of().formatHex(digest.digest(
                    Files.readAllBytes(root.resolve(file.getKey()))));
            assertEquals(file.getValue(), actual, file.getKey() + " is an immutable external input");
        }
    }

    private static String read(String relative) throws Exception {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        Path direct = cwd.resolve("src").resolve(relative);
        return Files.readString(Files.exists(direct) ? direct : cwd.resolve("server/src").resolve(relative));
    }

    private static Path projectRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("load-client")) ? cwd : cwd.getParent();
    }
}
