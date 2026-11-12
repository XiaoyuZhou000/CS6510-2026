import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The options of {@code SubscriberMain} (contracts/subscriber-client-and-reconciliation.md, "SubscriberMain"), as
 * {@code --key=value} arguments. {@code --resume} takes no value. A seed that is not given is generated with
 * {@link SecureRandom} and recorded in {@code snapshot.json} (FR-020).
 */
public final class SubscriberConfig {
    static final Set<String> OPTIONS = Set.of("subscribers", "seed", "skus-per-subscriber", "sku-distribution",
            "id-prefix", "assignments", "base-url", "bootstrap", "topic-prefix", "out", "resume", "stop-file");

    public final int subscribers;
    public final long seed;
    public final boolean seedGenerated;
    public final int skusPerSubscriber;
    public final String skuDistribution;
    public final String idPrefix;
    public final Path assignments;
    public final String baseUrl;
    public final String bootstrap;
    public final String topicPrefix;
    public final Path out;
    public final boolean resume;
    public final Path stopFile;

    private SubscriberConfig(Map<String, String> raw) {
        subscribers = intOption(raw, "subscribers", 100, 1, 9_999);
        if (raw.containsKey("seed")) {
            seed = parseLong(raw.get("seed"), "seed");
            seedGenerated = false;
        } else {
            seed = new SecureRandom().nextLong();
            seedGenerated = true;
        }
        skusPerSubscriber = intOption(raw, "skus-per-subscriber", 20, 1, 2_000);
        skuDistribution = raw.getOrDefault("sku-distribution", Assignment.UNIFORM);
        if (!skuDistribution.equals(Assignment.UNIFORM) && !skuDistribution.equals(Assignment.POPULARITY)) {
            throw new IllegalArgumentException("--sku-distribution must be uniform or popularity");
        }
        idPrefix = raw.getOrDefault("id-prefix", "sub");
        if (!idPrefix.matches("[A-Za-z0-9_-]{1,35}")) {
            throw new IllegalArgumentException("--id-prefix must match [A-Za-z0-9_-]{1,35}");
        }
        assignments = raw.containsKey("assignments") ? Path.of(raw.get("assignments")) : null;
        baseUrl = stripSlash(raw.getOrDefault("base-url", "http://localhost:8080"));
        bootstrap = raw.getOrDefault("bootstrap", "127.0.0.1:19092");
        topicPrefix = raw.getOrDefault("topic-prefix", "");
        String outText = raw.get("out");
        if (outText == null || outText.isBlank()) throw new IllegalArgumentException("--out=DIR is required");
        out = Path.of(outText).toAbsolutePath().normalize();
        resume = raw.containsKey("resume");
        stopFile = raw.containsKey("stop-file") ? Path.of(raw.get("stop-file")).toAbsolutePath() : out.resolve("stop");
    }

    public static SubscriberConfig parse(String[] args) {
        Map<String, String> raw = new LinkedHashMap<>();
        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                System.out.println(help());
                System.exit(0);
            }
            if (!arg.startsWith("--")) throw new IllegalArgumentException("Unrecognized argument: " + arg);
            String body = arg.substring(2);
            int equals = body.indexOf('=');
            String key = equals < 0 ? body : body.substring(0, equals);
            String value = equals < 0 ? "true" : body.substring(equals + 1);
            if (!OPTIONS.contains(key)) throw new IllegalArgumentException("Unknown option --" + key);
            if (equals < 0 && !key.equals("resume")) throw new IllegalArgumentException("--" + key + " needs a value");
            raw.put(key, value);
        }
        return new SubscriberConfig(raw);
    }

    /** The settings recorded in {@code snapshot.json}. */
    public RunFiles.Settings settings(int subscriberCount) {
        return new RunFiles.Settings(subscriberCount, assignments == null ? skusPerSubscriber : 0, skuDistribution,
                idPrefix, assignments == null ? RunFiles.SEEDED : RunFiles.FILE);
    }

    private static int intOption(Map<String, String> raw, String key, int fallback, int min, int max) {
        if (!raw.containsKey(key)) return fallback;
        long value = parseLong(raw.get(key), key);
        if (value < min || value > max) throw new IllegalArgumentException("--" + key + " must be " + min + "-" + max);
        return (int) value;
    }

    private static long parseLong(String text, String key) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " must be an integer: " + text);
        }
    }

    private static String stripSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    static String help() {
        return """
                Subscriber test application (feature 007)

                Usage: subscriber-client/run.sh SubscriberMain --out=DIR [--key=value ...]

                  --subscribers=N               subscribers and subscriber threads (default 100)
                  --seed=S                      assignment seed (default: generated and recorded)
                  --skus-per-subscriber=K       distinct SKUs per subscriber, 1-2000 (default 20)
                  --sku-distribution=uniform|popularity   (default uniform)
                  --id-prefix=P                 identities P-0001 ... P-N (default sub)
                  --assignments=FILE            explicit {"subscriberId": [skus]} instead of the seeded draw
                  --base-url=URL                gateway (default http://localhost:8080)
                  --bootstrap=HOST:PORT         Kafka (default 127.0.0.1:19092)
                  --topic-prefix=X              the services' KAFKA_TOPIC_PREFIX (default empty)
                  --out=DIR                     run directory (required)
                  --resume                      reuse DIR: no registration, resume after the recorded offsets
                  --stop-file=PATH              stop cleanly when PATH appears (default DIR/stop)
                """;
    }
}
