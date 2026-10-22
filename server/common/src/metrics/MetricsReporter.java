package metrics;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Opt-in periodic metrics summary (R21). With {@code PERF_METRICS=true} it prints one
 * {@link PerfMetrics#toJson} line every {@code PERF_METRICS_INTERVAL_SECONDS} (default 10) and once
 * more on {@link #stop()}. Disabled, it prints nothing; recording itself is never disabled.
 */
public final class MetricsReporter {
    private final String service;
    private final PrintStream out;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean stopped = new AtomicBoolean();

    private MetricsReporter(String service, PrintStream out, ScheduledExecutorService scheduler) {
        this.service = service;
        this.out = out;
        this.scheduler = scheduler;
    }

    public static MetricsReporter start(String service) {
        return start(service, System.getenv(), System.out);
    }

    public static MetricsReporter start(String service, Map<String, String> environment, PrintStream out) {
        if (service == null || service.isBlank()) throw new IllegalArgumentException("service name is required");
        if (!Boolean.parseBoolean(environment.getOrDefault("PERF_METRICS", "false"))) {
            return new MetricsReporter(service, out, null);
        }
        String configured = environment.get("PERF_METRICS_INTERVAL_SECONDS");
        long interval = configured == null || configured.isBlank() ? 10 : Long.parseLong(configured.trim());
        if (interval <= 0) throw new IllegalArgumentException("PERF_METRICS_INTERVAL_SECONDS must be positive");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, service + "-metrics-reporter");
            thread.setDaemon(true);
            return thread;
        });
        MetricsReporter reporter = new MetricsReporter(service, out, scheduler);
        scheduler.scheduleAtFixedRate(reporter::print, interval, interval, TimeUnit.SECONDS);
        return reporter;
    }

    public boolean enabled() { return scheduler != null; }

    /** Stops periodic output and prints the final summary once. */
    public void stop() {
        if (!stopped.compareAndSet(false, true) || scheduler == null) return;
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        print();
    }

    private void print() {
        out.println(PerfMetrics.toJson(service, System.currentTimeMillis()));
        out.flush();
    }
}
