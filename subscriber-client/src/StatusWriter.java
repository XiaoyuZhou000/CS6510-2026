import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code status.json}, rewritten every second through a temporary file and a rename (data-model.md section 5). It
 * holds per-subscriber {@code received}, {@code redeliveries}, and {@code lag} (channel end offset minus the
 * consumer position; -1 while unknown), plus totals. The harness reads {@code totals.lag} for drain detection; an
 * unknown lag keeps {@code totals.lagKnown} false.
 */
public final class StatusWriter {
    public record Stats(long received, long redeliveries, long undecodable, long lag) { }

    private final Path file;

    public StatusWriter(Path file) {
        this.file = file;
    }

    public void write(Map<String, Stats> perSubscriber) throws IOException {
        RunFiles.writeAtomically(file, render(System.currentTimeMillis(), perSubscriber));
    }

    public static String render(long updatedAtMs, Map<String, Stats> perSubscriber) {
        long received = 0;
        long redeliveries = 0;
        long undecodable = 0;
        long lag = 0;
        boolean lagKnown = true;
        Map<String, Object> subscribers = new LinkedHashMap<>();
        for (Map.Entry<String, Stats> entry : new TreeMap<>(perSubscriber).entrySet()) {
            Stats stats = entry.getValue();
            received += stats.received();
            redeliveries += stats.redeliveries();
            undecodable += stats.undecodable();
            if (stats.lag() < 0) lagKnown = false;
            else lag += stats.lag();
            subscribers.put(entry.getKey(), Json.object("received", stats.received(), "redeliveries", stats.redeliveries(),
                    "lag", stats.lag()));
        }
        return Json.pretty(Json.object(
                "updatedAtMs", updatedAtMs,
                "totals", Json.object("subscribers", (long) perSubscriber.size(), "received", received,
                        "redeliveries", redeliveries, "undecodable", undecodable, "lag", lag, "lagKnown", lagKnown),
                "subscribers", subscribers));
    }
}
