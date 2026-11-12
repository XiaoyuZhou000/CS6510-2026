import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** T064: seeded SKU assignment is reproducible and nested across subscriber counts (FR-020, SC-013, research N10). */
class AssignmentDeterminismTest {

    static List<String> catalog(int size) {
        List<String> skus = new ArrayList<>();
        for (int i = 1; i <= size; i++) skus.add(String.format("SKU-%06d", i));
        return skus;
    }

    /** The catalog in a scrambled order, as a server could return it; assignment must not depend on that order. */
    static List<String> scrambled(List<String> catalog, long seed) {
        List<String> copy = new ArrayList<>(catalog);
        Collections.shuffle(copy, new Random(seed));
        return copy;
    }

    @Test
    void sameSeedAndSettingsGiveByteIdenticalSnapshot(@TempDir Path dir) throws Exception {
        List<String> first = Assignment.sortedCatalog(scrambled(catalog(2000), 1));
        List<String> second = Assignment.sortedCatalog(scrambled(catalog(2000), 2));
        RunFiles.Settings settings = new RunFiles.Settings(100, 20, Assignment.UNIFORM, "sub", RunFiles.SEEDED);
        Path a = Files.createDirectories(dir.resolve("a"));
        Path b = Files.createDirectories(dir.resolve("b"));
        RunFiles.writeSnapshot(a, 20261111L, settings, first,
                Assignment.seeded(first, 100, 20, Assignment.UNIFORM, 20261111L, "sub"));
        RunFiles.writeSnapshot(b, 20261111L, settings, second,
                Assignment.seeded(second, 100, 20, Assignment.UNIFORM, 20261111L, "sub"));
        byte[] left = Files.readAllBytes(a.resolve("snapshot.json"));
        byte[] right = Files.readAllBytes(b.resolve("snapshot.json"));
        assertArrayEquals(left, right);

        String text = new String(left, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(text.contains("\"seed\": 20261111"), text.substring(0, 200));
        assertTrue(text.contains("\"sha256\": \"" + RunFiles.catalogSha256(first) + "\""));

        Path c = Files.createDirectories(dir.resolve("c"));
        RunFiles.writeSnapshot(c, 20261112L, settings, first,
                Assignment.seeded(first, 100, 20, Assignment.UNIFORM, 20261112L, "sub"));
        assertNotEquals(new String(left), Files.readString(c.resolve("snapshot.json")), "another seed gives another snapshot");
    }

    @Test
    void subscriberSetsAreNestedAcrossSubscriberCounts() {
        List<String> skus = catalog(2000);
        List<Assignment.Subscriber> ten = Assignment.seeded(skus, 10, 20, Assignment.UNIFORM, 99L, "sub");
        List<Assignment.Subscriber> hundred = Assignment.seeded(skus, 100, 20, Assignment.UNIFORM, 99L, "sub");
        assertEquals(10, ten.size());
        assertEquals(100, hundred.size());
        for (int i = 0; i < 10; i++) assertEquals(ten.get(i), hundred.get(i));

        List<Assignment.Subscriber> popular10 = Assignment.seeded(skus, 10, 5, Assignment.POPULARITY, 99L, "sub");
        List<Assignment.Subscriber> popular25 = Assignment.seeded(skus, 25, 5, Assignment.POPULARITY, 99L, "sub");
        assertEquals(popular10, popular25.subList(0, 10));
    }

    @Test
    void uniformGivesKDistinctCatalogSkus() {
        List<String> skus = catalog(2000);
        Set<String> known = new HashSet<>(skus);
        for (Assignment.Subscriber subscriber : Assignment.seeded(skus, 100, 20, Assignment.UNIFORM, 7L, "sub")) {
            assertEquals(20, subscriber.skus().size());
            assertEquals(20, new HashSet<>(subscriber.skus()).size(), "distinct");
            assertTrue(known.containsAll(subscriber.skus()));
            List<String> sorted = new ArrayList<>(subscriber.skus());
            Collections.sort(sorted);
            assertEquals(sorted, subscriber.skus(), "sorted ascending");
        }
        // K equal to the catalog size takes every SKU.
        assertEquals(catalog(30), Assignment.draw(catalog(30), 30, Assignment.UNIFORM, 1L, 1));
        assertEquals(catalog(30), Assignment.draw(catalog(30), 30, Assignment.POPULARITY, 1L, 1));
        // Uniform draws are not biased toward the first ranks.
        double meanRank = meanRank(Assignment.seeded(skus, 200, 20, Assignment.UNIFORM, 11L, "sub"));
        assertTrue(meanRank > 800 && meanRank < 1200, "uniform mean rank " + meanRank);
    }

    @Test
    void popularityGivesKDistinctSkusBiasedTowardLowRanks() {
        List<String> skus = catalog(2000);
        List<Assignment.Subscriber> subscribers = Assignment.seeded(skus, 200, 20, Assignment.POPULARITY, 11L, "sub");
        for (Assignment.Subscriber subscriber : subscribers) {
            assertEquals(20, subscriber.skus().size());
            assertEquals(20, new HashSet<>(subscriber.skus()).size(), "distinct");
        }
        double meanRank = meanRank(subscribers);
        assertTrue(meanRank < 500, "popularity mean rank " + meanRank);
        long topTen = subscribers.stream().filter(s -> s.skus().contains("SKU-000001")).count();
        assertTrue(topTen > 100, "the top-ranked SKU is drawn by most subscribers: " + topTen);
    }

    private static double meanRank(List<Assignment.Subscriber> subscribers) {
        long total = 0;
        long count = 0;
        for (Assignment.Subscriber subscriber : subscribers) {
            for (String sku : subscriber.skus()) {
                total += Integer.parseInt(sku.substring(4));
                count++;
            }
        }
        return (double) total / count;
    }

    @Test
    void assignmentsFileOverridesTheSeededAssignment(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("b.json");
        Files.writeString(file, "{\"S2\": [\"SKU-000002\"], \"S1\": [\"SKU-000002\", \"SKU-000001\"]}");
        List<Assignment.Subscriber> subscribers = Assignment.fromFile(file, catalog(10));
        assertEquals(List.of(new Assignment.Subscriber("S1", List.of("SKU-000001", "SKU-000002")),
                new Assignment.Subscriber("S2", List.of("SKU-000002"))), subscribers);

        RunFiles.writeSnapshot(dir, 5L, new RunFiles.Settings(2, 0, Assignment.UNIFORM, "sub", RunFiles.FILE),
                catalog(10), subscribers);
        RunFiles.Snapshot snapshot = RunFiles.readSnapshot(dir);
        assertEquals(Map.of("S1", List.of("SKU-000001", "SKU-000002"), "S2", List.of("SKU-000002")), snapshot.subscribers());
        assertEquals(RunFiles.FILE, snapshot.settings().get("assignment"));

        Files.writeString(file, "{\"S1\": [\"SKU-999999\"]}");
        assertThrows(IllegalArgumentException.class, () -> Assignment.fromFile(file, catalog(10)));
        Files.writeString(file, "{\"S1\": []}");
        assertThrows(IllegalArgumentException.class, () -> Assignment.fromFile(file, catalog(10)));
    }

    @Test
    void identitiesArePrefixDashFourDigits() {
        List<Assignment.Subscriber> subscribers = Assignment.seeded(catalog(100), 12, 3, Assignment.UNIFORM, 1L, "sub");
        assertEquals("sub-0001", subscribers.get(0).id());
        assertEquals("sub-0012", subscribers.get(11).id());
        assertEquals("abc-0100", Assignment.subscriberId("abc", 100));
        assertEquals("sub-9999", Assignment.subscriberId("sub", 9999));
    }

    @Test
    void mix64IsSplitMixFinalizer() {
        // SplitMix64's finalizer maps 0 to 0 and is a bijection; known value for input 1.
        assertEquals(0L, Assignment.mix64(0L));
        assertEquals(0x5692161d100b05e5L, Assignment.mix64(1L));
    }
}
