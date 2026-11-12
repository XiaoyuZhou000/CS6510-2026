import java.io.IOException;
import java.io.PrintStream;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * The subscriber test application (FR-019-FR-021, contracts/subscriber-client-and-reconciliation.md "Lifecycle"):
 *
 * <ol>
 *   <li>reads the catalog through {@code GET /items} and writes {@code snapshot.json};</li>
 *   <li>registers every subscriber (8 in parallel, idempotent retries);</li>
 *   <li>waits until every channel is ready;</li>
 *   <li>starts one thread per subscriber at offset 0;</li>
 *   <li>writes {@code ready.json} with the highest confirmed subscription version;</li>
 *   <li>runs until the stop file appears, rewriting {@code status.json} every second, then stops cleanly.</li>
 * </ol>
 *
 * With {@code --resume} it skips steps 1-3 and 5, reusing the snapshot and the channels recorded in
 * {@code ready.json}, and each thread resumes after its highest recorded offset.
 *
 * <p>Exit codes: 0 clean stop; 2 invalid options or run directory; 3 registration or channel failure; 4 an
 * unrecoverable consumer or receipt error.
 */
public final class SubscriberMain {
    static final int EXIT_OK = 0;
    static final int EXIT_USAGE = 2;
    static final int EXIT_REGISTRATION = 3;
    static final int EXIT_CONSUMER = 4;
    static final String CHANNEL_START = "selfcheckout.notify.";

    private SubscriberMain() { }

    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    static int run(String[] args, PrintStream log) {
        SubscriberConfig config;
        try {
            config = SubscriberConfig.parse(args);
        } catch (IllegalArgumentException e) {
            log.println("subscriber-client: " + e.getMessage());
            log.println(SubscriberConfig.help());
            return EXIT_USAGE;
        }
        try {
            return run(config, log);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.println("subscriber-client: interrupted");
            return EXIT_CONSUMER;
        } catch (IOException e) {
            log.println("subscriber-client: " + e);
            return EXIT_USAGE;
        }
    }

    static int run(SubscriberConfig config, PrintStream log) throws IOException, InterruptedException {
        Path out = Files.createDirectories(config.out);
        Path receipts = out.resolve("receipts");
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        Registrar registrar = new Registrar(http, config.baseUrl, log);

        List<String> subscriberIds = new ArrayList<>();
        Map<String, String> channels = new TreeMap<>();
        Map<String, Registrar.Registered> registered = null;
        if (config.resume) {
            if (!Files.exists(out.resolve("snapshot.json")) || !Files.exists(out.resolve("ready.json"))) {
                log.println("subscriber-client: --resume needs snapshot.json and ready.json in " + out);
                return EXIT_USAGE;
            }
            RunFiles.Snapshot snapshot = RunFiles.readSnapshot(out);
            RunFiles.Ready ready = RunFiles.readReady(out);
            subscriberIds.addAll(snapshot.subscribers().keySet());
            for (String id : subscriberIds) {
                String channel = ready.channels().get(id);
                if (channel == null) {
                    try {
                        channel = registrar.get(id).channel();
                    } catch (Registrar.RegistrationException e) {
                        log.println("subscriber-client: " + e.getMessage());
                        return EXIT_REGISTRATION;
                    }
                }
                channels.put(id, channel);
            }
            log.println("resuming " + subscriberIds.size() + " subscribers in " + out);
        } else {
            if (Files.exists(out.resolve("snapshot.json")) || hasReceipts(receipts)) {
                log.println("subscriber-client: " + out + " already holds a run; use --resume or a new --out");
                return EXIT_USAGE;
            }
            List<Assignment.Subscriber> subscribers;
            try {
                List<String> catalog = Assignment.sortedCatalog(registrar.fetchCatalog());
                subscribers = config.assignments == null
                        ? Assignment.seeded(catalog, config.subscribers, config.skusPerSubscriber, config.skuDistribution,
                                config.seed, config.idPrefix)
                        : Assignment.fromFile(config.assignments, catalog);
                RunFiles.writeSnapshot(out, config.seed, config.settings(subscribers.size()), catalog, subscribers);
                log.println("snapshot.json: " + subscribers.size() + " subscribers, seed " + config.seed
                        + (config.seedGenerated ? " (generated)" : ""));
                registered = registrar.awaitChannelsReady(registrar.registerAll(subscribers), Duration.ofSeconds(60));
            } catch (Registrar.RegistrationException e) {
                log.println("subscriber-client: registration failed: " + e.getMessage());
                return EXIT_REGISTRATION;
            } catch (IllegalArgumentException e) {
                log.println("subscriber-client: " + e.getMessage());
                return EXIT_USAGE;
            }
            String expected = config.topicPrefix + CHANNEL_START;
            for (Registrar.Registered view : registered.values()) {
                if (view.channel() == null || !view.channel().startsWith(expected)) {
                    log.println("subscriber-client: channel " + view.channel() + " of " + view.subscriberId()
                            + " does not start with " + expected + "; check --topic-prefix");
                    return EXIT_USAGE;
                }
                subscriberIds.add(view.subscriberId());
                channels.put(view.subscriberId(), view.channel());
            }
            log.println("registered " + registered.size() + " subscribers; every channel is ready");
        }
        startAndWriteReady(config, log, out, receipts, subscriberIds, channels, registered);
        return runUntilStopped(config, log, out);
    }

    private static boolean hasReceipts(Path receipts) throws IOException {
        if (!Files.isDirectory(receipts)) return false;
        try (Stream<Path> files = Files.list(receipts)) {
            return files.findAny().isPresent();
        }
    }

    private static final List<SubscriberThread> threads = new ArrayList<>();
    private static final List<Thread> runners = new ArrayList<>();

    private static void startAndWriteReady(SubscriberConfig config, PrintStream log, Path out, Path receipts,
                                           List<String> subscriberIds, Map<String, String> channels,
                                           Map<String, Registrar.Registered> registered) throws IOException {
        Files.deleteIfExists(config.stopFile);
        for (String id : subscriberIds) {
            SubscriberThread subscriber = new SubscriberThread(id, channels.get(id), config.bootstrap,
                    new ReceiptWriter(receipts, id), log);
            Thread runner = new Thread(subscriber, "subscriber-" + id);
            threads.add(subscriber);
            runners.add(runner);
        }
        for (Thread runner : runners) runner.start();
        if (registered != null) {
            Map<String, Long> versions = new LinkedHashMap<>();
            long highest = 0;
            for (Registrar.Registered view : registered.values()) {
                versions.put(view.subscriberId(), view.version());
                highest = Math.max(highest, view.version());
            }
            RunFiles.writeReady(out, new RunFiles.Ready(Instant.now(), highest, versions, channels));
            log.println("ready.json: subscriptionVersion " + highest + "; " + threads.size() + " subscriber threads started");
        } else {
            log.println(threads.size() + " subscriber threads resumed");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(SubscriberMain::stopAll, "subscriber-shutdown"));
    }

    private static int runUntilStopped(SubscriberConfig config, PrintStream log, Path out) throws InterruptedException {
        StatusWriter status = new StatusWriter(out.resolve("status.json"));
        long nextStatus = 0;
        int exit = EXIT_OK;
        while (true) {
            if (Files.exists(config.stopFile)) {
                log.println("stop file " + config.stopFile + " found; stopping");
                break;
            }
            SubscriberThread failed = threads.stream().filter(t -> t.failure() != null).findFirst().orElse(null);
            if (failed != null) {
                log.println("subscriber-client: " + failed.subscriberId() + " failed: " + failed.failure());
                exit = EXIT_CONSUMER;
                break;
            }
            long now = System.currentTimeMillis();
            if (now >= nextStatus) {
                writeStatus(status, log);
                nextStatus = now + 1_000;
            }
            Thread.sleep(200);
        }
        stopAll();
        writeStatus(status, log);
        for (SubscriberThread thread : threads) {
            if (thread.failure() != null) exit = EXIT_CONSUMER;
        }
        log.println("stopped; exit code " + exit);
        return exit;
    }

    private static void writeStatus(StatusWriter status, PrintStream log) {
        Map<String, StatusWriter.Stats> stats = new TreeMap<>();
        for (SubscriberThread thread : threads) stats.put(thread.subscriberId(), thread.stats());
        try {
            status.write(stats);
        } catch (IOException e) {
            log.println("subscriber-client: cannot write status.json: " + e);
        }
    }

    /** Stops every thread and waits for each to flush its receipts; safe to call more than once. */
    private static synchronized void stopAll() {
        for (SubscriberThread thread : threads) thread.stop();
        for (Thread runner : runners) {
            try {
                runner.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
