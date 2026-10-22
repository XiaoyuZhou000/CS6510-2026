package metrics;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Periodic operational status line (FR-039). Every {@code STATUS_INTERVAL_SECONDS} (default 30) it prints
 * the line its service's summary supplies, for example its pending recovery work. A failing summary prints
 * one {@code unavailable} line instead, and the next interval tries again.
 */
public final class StatusReporter {
    public static final long DEFAULT_INTERVAL_SECONDS = 30;

    private final String service;
    private final Supplier<String> summary;
    private final PrintStream out;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean stopped = new AtomicBoolean();

    private StatusReporter(String service, Supplier<String> summary, PrintStream out, ScheduledExecutorService scheduler) {
        this.service = service;
        this.summary = summary;
        this.out = out;
        this.scheduler = scheduler;
    }

    public static StatusReporter start(String service, Supplier<String> summary) {
        return start(service, summary, System.getenv(), System.out);
    }

    public static StatusReporter start(String service, Supplier<String> summary, Map<String, String> environment,
                                       PrintStream out) {
        if (service == null || service.isBlank()) throw new IllegalArgumentException("service name is required");
        if (summary == null) throw new IllegalArgumentException("summary is required");
        String configured = environment.get("STATUS_INTERVAL_SECONDS");
        long interval = configured == null || configured.isBlank() ? DEFAULT_INTERVAL_SECONDS : Long.parseLong(configured.trim());
        if (interval <= 0) throw new IllegalArgumentException("STATUS_INTERVAL_SECONDS must be positive");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, service + "-status-reporter");
            thread.setDaemon(true);
            return thread;
        });
        StatusReporter reporter = new StatusReporter(service, summary, out, scheduler);
        scheduler.scheduleWithFixedDelay(reporter::print, interval, interval, TimeUnit.SECONDS);
        return reporter;
    }

    /** Stops periodic output; a line being printed finishes first. */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) return;
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Prints one summary line now, as each interval does. */
    public void print() {
        String line;
        try {
            line = summary.get();
        } catch (RuntimeException failure) {
            line = "status service=" + service + " unavailable error=" + failure.getClass().getSimpleName();
        }
        out.println(line);
        out.flush();
    }
}
