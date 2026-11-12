import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Registers the subscribers through the gateway's public routes (contracts/notification-http.md) and waits for
 * their channels.
 *
 * <ul>
 *   <li>8 registrations run in parallel. Registration is idempotent, so a 5xx answer or a connection failure,
 *       whose outcome is unknown, is retried up to 10 times, 200 ms apart.</li>
 *   <li>Any 4xx fails the run: the request itself is wrong (a conflicting subscriber, an SKU outside the catalog).</li>
 *   <li>Then {@code GET /notifications/subscribers/{id}} is polled until every {@code channelReady} is true, for at
 *       most 60 s.</li>
 * </ul>
 */
public final class Registrar {
    public static final int PARALLEL = 8;
    public static final int RETRIES = 10;
    public static final Duration BACKOFF = Duration.ofMillis(200);

    public record Registered(String subscriberId, long version, String channel, boolean channelReady) { }

    public static final class RegistrationException extends Exception {
        public RegistrationException(String message) { super(message); }
    }

    private final HttpClient client;
    private final String baseUrl;
    private final PrintStream log;

    public Registrar(HttpClient client, String baseUrl, PrintStream log) {
        this.client = client;
        this.baseUrl = baseUrl;
        this.log = log;
    }

    /** The catalog SKUs from the public {@code GET /items}. */
    public List<String> fetchCatalog() throws RegistrationException, InterruptedException {
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(baseUrl + "/items")).GET(), "GET /items");
        if (response.statusCode() != 200) {
            throw new RegistrationException("GET /items answered " + response.statusCode() + ": " + response.body());
        }
        List<String> skus = new ArrayList<>();
        for (Object item : Json.getList(Json.parseObject(response.body()), "items")) {
            skus.add(Json.getString(Json.asObject(item), "sku"));
        }
        if (skus.isEmpty()) throw new RegistrationException("GET /items returned an empty catalog");
        return skus;
    }

    public Map<String, Registered> registerAll(List<Assignment.Subscriber> subscribers)
            throws RegistrationException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(PARALLEL, runnable -> {
            Thread thread = new Thread(runnable, "registrar");
            thread.setDaemon(true);
            return thread;
        });
        try {
            List<Future<Registered>> futures = new ArrayList<>();
            for (Assignment.Subscriber subscriber : subscribers) futures.add(pool.submit(() -> register(subscriber)));
            Map<String, Registered> registered = new TreeMap<>();
            for (Future<Registered> future : futures) {
                try {
                    Registered result = future.get();
                    registered.put(result.subscriberId(), result);
                } catch (ExecutionException e) {
                    for (Future<Registered> other : futures) other.cancel(true);
                    if (e.getCause() instanceof RegistrationException failure) throw failure;
                    throw new RegistrationException("Registration failed: " + e.getCause());
                }
            }
            return registered;
        } finally {
            pool.shutdownNow();
        }
    }

    public Registered register(Assignment.Subscriber subscriber) throws RegistrationException, InterruptedException {
        String body = Json.compact(Json.object("subscriberId", subscriber.id(), "skus", new ArrayList<Object>(subscriber.skus())));
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + "/notifications/subscribers"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        HttpResponse<String> response = send(request, "register " + subscriber.id());
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            throw new RegistrationException("Registering " + subscriber.id() + " answered " + response.statusCode()
                    + ": " + response.body());
        }
        return view(response.body());
    }

    public Registered get(String subscriberId) throws RegistrationException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + "/notifications/subscribers/"
                + URLEncoder.encode(subscriberId, StandardCharsets.UTF_8))).GET();
        HttpResponse<String> response = send(request, "GET subscriber " + subscriberId);
        if (response.statusCode() != 200) {
            throw new RegistrationException("GET subscriber " + subscriberId + " answered " + response.statusCode()
                    + ": " + response.body());
        }
        return view(response.body());
    }

    /** Polls until every channel is ready; returns the final views. */
    public Map<String, Registered> awaitChannelsReady(Map<String, Registered> registered, Duration timeout)
            throws RegistrationException, InterruptedException {
        Map<String, Registered> current = new TreeMap<>(registered);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            List<String> waiting = new ArrayList<>();
            for (Registered view : current.values()) if (!view.channelReady()) waiting.add(view.subscriberId());
            if (waiting.isEmpty()) return current;
            if (System.nanoTime() > deadline) {
                throw new RegistrationException(waiting.size() + " channel(s) not ready after " + timeout.toSeconds()
                        + " s, for example " + waiting.get(0));
            }
            Thread.sleep(250);
            for (String id : waiting) {
                Registered view = get(id);
                current.put(id, new Registered(id, current.get(id).version(), view.channel(), view.channelReady()));
            }
        }
    }

    private static Registered view(String body) {
        Map<String, Object> view = Json.parseObject(body);
        return new Registered(Json.getString(view, "subscriberId"), Json.getLong(view, "subscriptionVersion"),
                Json.getString(view, "channel"), Json.getBoolean(view, "channelReady"));
    }

    /** Sends with the retry policy: 5xx and connection failures are retried; anything else is returned. */
    private HttpResponse<String> send(HttpRequest.Builder request, String what)
            throws RegistrationException, InterruptedException {
        HttpRequest built = request.timeout(Duration.ofSeconds(10)).build();
        String last = null;
        for (int attempt = 0; attempt <= RETRIES; attempt++) {
            if (attempt > 0) Thread.sleep(BACKOFF.toMillis());
            try {
                HttpResponse<String> response = client.send(built, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() < 500) return response;
                last = response.statusCode() + " " + response.body();
            } catch (IOException e) {
                last = e.toString();
            }
            log.println("retrying " + what + " after: " + last);
        }
        throw new RegistrationException(what + " failed after " + RETRIES + " retries: " + last);
    }
}
