import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code snapshot.json} and {@code ready.json} (data-model.md section 5).
 *
 * <p>The snapshot holds the seed, the settings, the catalog's SHA-256, and the subscribers sorted by identity, with
 * a fixed key order and no timestamp, so the same seed and settings always give a byte-identical file (SC-013).
 * {@code ready.json} is written once every registration is confirmed and every channel exists: {@code readyAt},
 * the highest confirmed {@code subscriptionVersion}, the per-subscriber confirmed versions, and the channel topics
 * (used by {@code --resume}).
 */
public final class RunFiles {
    public static final String SEEDED = "seeded";
    public static final String FILE = "file";

    public record Settings(int subscribers, int skusPerSubscriber, String skuDistribution, String idPrefix,
                           String assignment) { }

    public record Snapshot(long seed, Map<String, Object> settings, Map<String, List<String>> subscribers) { }

    public record Ready(Instant readyAt, long subscriptionVersion, Map<String, Long> versions, Map<String, String> channels) { }

    private RunFiles() { }

    public static String catalogSha256(List<String> sortedCatalog) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(String.join("\n", sortedCatalog).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String snapshotJson(long seed, Settings settings, List<String> sortedCatalog,
                                      List<Assignment.Subscriber> subscribers) {
        List<Assignment.Subscriber> byId = new ArrayList<>(subscribers);
        byId.sort((a, b) -> a.id().compareTo(b.id()));
        List<Object> entries = new ArrayList<>(byId.size());
        for (Assignment.Subscriber subscriber : byId) {
            entries.add(Json.object("id", subscriber.id(), "skus", new ArrayList<Object>(subscriber.skus())));
        }
        return Json.pretty(Json.object(
                "seed", seed,
                "settings", Json.object(
                        "subscribers", (long) settings.subscribers(),
                        "skusPerSubscriber", (long) settings.skusPerSubscriber(),
                        "skuDistribution", settings.skuDistribution(),
                        "idPrefix", settings.idPrefix(),
                        "assignment", settings.assignment()),
                "catalog", Json.object("size", (long) sortedCatalog.size(), "sha256", catalogSha256(sortedCatalog)),
                "subscribers", entries));
    }

    public static void writeSnapshot(Path dir, long seed, Settings settings, List<String> sortedCatalog,
                                     List<Assignment.Subscriber> subscribers) throws IOException {
        writeAtomically(dir.resolve("snapshot.json"), snapshotJson(seed, settings, sortedCatalog, subscribers));
    }

    public static Snapshot readSnapshot(Path dir) throws IOException {
        Map<String, Object> root = Json.parseObject(Files.readString(dir.resolve("snapshot.json"), StandardCharsets.UTF_8));
        Map<String, List<String>> subscribers = new TreeMap<>();
        for (Object entry : Json.getList(root, "subscribers")) {
            Map<String, Object> subscriber = Json.asObject(entry);
            subscribers.put(Json.getString(subscriber, "id"), Json.strings(Json.getList(subscriber, "skus")));
        }
        return new Snapshot(Json.getLong(root, "seed"), Json.getObject(root, "settings"), subscribers);
    }

    public static void writeReady(Path dir, Ready ready) throws IOException {
        Map<String, Object> versions = new LinkedHashMap<>();
        new TreeMap<>(ready.versions()).forEach(versions::put);
        Map<String, Object> channels = new LinkedHashMap<>();
        new TreeMap<>(ready.channels()).forEach(channels::put);
        writeAtomically(dir.resolve("ready.json"), Json.pretty(Json.object(
                "readyAt", ready.readyAt().toString(),
                "subscriptionVersion", ready.subscriptionVersion(),
                "subscribers", versions,
                "channels", channels)));
    }

    public static Ready readReady(Path dir) throws IOException {
        Map<String, Object> root = Json.parseObject(Files.readString(dir.resolve("ready.json"), StandardCharsets.UTF_8));
        Map<String, Long> versions = new TreeMap<>();
        Map<String, Object> rawVersions = Json.getObject(root, "subscribers");
        if (rawVersions != null) rawVersions.forEach((id, v) -> versions.put(id, ((Number) v).longValue()));
        Map<String, String> channels = new TreeMap<>();
        Map<String, Object> rawChannels = Json.getObject(root, "channels");
        if (rawChannels != null) rawChannels.forEach((id, topic) -> channels.put(id, String.valueOf(topic)));
        return new Ready(Instant.parse(Json.getString(root, "readyAt")), Json.getLong(root, "subscriptionVersion"),
                versions, channels);
    }

    /**
     * Writes through a temporary file and a rename, so a reader (the harness polls {@code status.json}) never sees
     * a partial file. On Windows a reader holding the target open can make the rename fail briefly, so it is retried.
     */
    public static void writeAtomically(Path target, String content) throws IOException {
        Path parent = target.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        Path temporary = Files.createTempFile(parent, target.getFileName().toString(), ".tmp");
        try {
            Files.writeString(temporary, content, StandardCharsets.UTF_8);
            for (int attempt = 1; ; attempt++) {
                try {
                    try {
                        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    } catch (AtomicMoveNotSupportedException e) {
                        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                    return;
                } catch (AccessDeniedException e) {
                    if (attempt >= 20) throw e;
                    try {
                        Thread.sleep(25);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
