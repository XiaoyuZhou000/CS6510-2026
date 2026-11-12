import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Seeded SKU assignment (research N10, FR-020, SC-013). The catalog SKUs are sorted ascending; subscriber {@code i}
 * (1-based) draws its K distinct SKUs with its own {@code SplittableRandom(mix64(seed + i))}, so its set never
 * depends on how many subscribers there are, and the sets are nested across subscriber counts.
 *
 * <ul>
 *   <li>{@code uniform}: a partial Fisher-Yates shuffle of the sorted catalog, taking the first K.</li>
 *   <li>{@code popularity}: weighted sampling without replacement with the Load Client's Zipf weights, 1/rank,
 *       where rank 1 is the first SKU of the sorted catalog.</li>
 * </ul>
 * Each subscriber's SKUs are returned sorted ascending.
 */
public final class Assignment {
    public static final String UNIFORM = "uniform";
    public static final String POPULARITY = "popularity";

    public record Subscriber(String id, List<String> skus) {
        public Subscriber {
            skus = List.copyOf(skus);
        }
    }

    private Assignment() { }

    /** SplitMix64's finalizer (the mixing function of {@link SplittableRandom}). */
    public static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    public static String subscriberId(String prefix, int index) {
        return String.format("%s-%04d", prefix, index);
    }

    public static List<String> sortedCatalog(Collection<String> skus) {
        return List.copyOf(new TreeSet<>(skus));
    }

    public static List<Subscriber> seeded(List<String> sortedCatalog, int subscribers, int skusPerSubscriber,
                                          String distribution, long seed, String prefix) {
        if (skusPerSubscriber > sortedCatalog.size()) {
            throw new IllegalArgumentException("--skus-per-subscriber=" + skusPerSubscriber + " exceeds the catalog size "
                    + sortedCatalog.size());
        }
        List<Subscriber> result = new ArrayList<>(subscribers);
        for (int i = 1; i <= subscribers; i++) {
            result.add(new Subscriber(subscriberId(prefix, i), draw(sortedCatalog, skusPerSubscriber, distribution, seed, i)));
        }
        return result;
    }

    /** Subscriber {@code index}'s K SKUs, sorted. */
    public static List<String> draw(List<String> sortedCatalog, int k, String distribution, long seed, int index) {
        SplittableRandom random = new SplittableRandom(mix64(seed + index));
        List<String> drawn = switch (distribution) {
            case UNIFORM -> uniform(sortedCatalog, k, random);
            case POPULARITY -> popularity(sortedCatalog, k, random);
            default -> throw new IllegalArgumentException("Unknown SKU distribution: " + distribution);
        };
        List<String> sorted = new ArrayList<>(drawn);
        sorted.sort(null);
        return sorted;
    }

    private static List<String> uniform(List<String> catalog, int k, SplittableRandom random) {
        String[] pool = catalog.toArray(new String[0]);
        for (int j = 0; j < k; j++) {
            int pick = j + random.nextInt(pool.length - j);
            String swap = pool[j];
            pool[j] = pool[pick];
            pool[pick] = swap;
        }
        return List.of(pool).subList(0, k);
    }

    private static List<String> popularity(List<String> catalog, int k, SplittableRandom random) {
        int n = catalog.size();
        double[] weights = new double[n];
        double remaining = 0;
        for (int r = 0; r < n; r++) {
            weights[r] = 1.0 / (r + 1);
            remaining += weights[r];
        }
        List<String> drawn = new ArrayList<>(k);
        for (int j = 0; j < k; j++) {
            double target = random.nextDouble() * remaining;
            int pick = -1;
            double running = 0;
            for (int r = 0; r < n; r++) {
                if (weights[r] == 0) continue;
                pick = r;
                running += weights[r];
                if (running > target) break;
            }
            drawn.add(catalog.get(pick));
            remaining -= weights[pick];
            weights[pick] = 0;
        }
        return drawn;
    }

    /**
     * An explicit {@code {"subscriberId": ["SKU", ...]}} file for small functional scenarios; it replaces the seeded
     * assignment. Subscribers are sorted by identity and their SKUs ascending; every SKU must be in the catalog.
     */
    public static List<Subscriber> fromFile(Path file, Collection<String> catalog) throws IOException {
        Map<String, Object> raw = Json.parseObject(Files.readString(file, StandardCharsets.UTF_8));
        Set<String> known = new HashSet<>(catalog);
        TreeMap<String, List<String>> sorted = new TreeMap<>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            if (!entry.getKey().matches("[A-Za-z0-9_-]{1,40}")) {
                throw new IllegalArgumentException("Invalid subscriber identity in " + file + ": " + entry.getKey());
            }
            if (!(entry.getValue() instanceof List<?> list) || list.isEmpty()) {
                throw new IllegalArgumentException("Subscriber " + entry.getKey() + " needs a non-empty SKU list in " + file);
            }
            TreeSet<String> skus = new TreeSet<>();
            for (Object sku : list) {
                if (!known.contains(String.valueOf(sku))) {
                    throw new IllegalArgumentException("Subscriber " + entry.getKey() + " has an SKU outside the catalog: " + sku);
                }
                skus.add(String.valueOf(sku));
            }
            sorted.put(entry.getKey(), List.copyOf(skus));
        }
        List<Subscriber> result = new ArrayList<>();
        sorted.forEach((id, skus) -> result.add(new Subscriber(id, skus)));
        return result;
    }
}
