package analytics;

import database.PopularWindowStore;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Application-facing facade and lifecycle owner for the three-stage analytics pipeline. */
public final class AnalyticsService implements AnalyticsOperations {
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5_000L;
    private static final int DEFAULT_BACKLOG_THRESHOLD = 1_000;

    private final Object lifecycleLock = new Object();
    private final PopularWindowStore windowStore;
    private final AnalyticsLog log;
    private final long shutdownTimeoutMillis;
    private final BlockingQueue<PipelineMessage.IngressMessage> ingressQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PipelineMessage.WindowMessage> windowQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<PipelineMessage.RankingMessage> rankingQueue = new LinkedBlockingQueue<>();
    private final Thread windowWorker;
    private final Thread rankingWorker;
    private final Thread persistenceWorker;
    private volatile State state = State.NEW;
    private boolean endMarkerSent;
    private long nextIngressBacklogThreshold;
    private int ingressHighWater;

    public AnalyticsService(PopularWindowStore windowStore) {
        this(windowStore, AnalyticsLog.stderr(), DEFAULT_BACKLOG_THRESHOLD,
                DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, WorkerDecorator.identity());
    }

    public AnalyticsService(PopularWindowStore windowStore, long shutdownTimeoutMillis) {
        this(windowStore, AnalyticsLog.stderr(), DEFAULT_BACKLOG_THRESHOLD,
                shutdownTimeoutMillis, WorkerDecorator.identity());
    }

    public AnalyticsService(PopularWindowStore windowStore, AnalyticsLog log, int initialBacklogThreshold) {
        this(windowStore, log, initialBacklogThreshold,
                DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, WorkerDecorator.identity());
    }

    public AnalyticsService(PopularWindowStore windowStore, AnalyticsLog log,
            int initialBacklogThreshold, long shutdownTimeoutMillis) {
        this(windowStore, log, initialBacklogThreshold,
                shutdownTimeoutMillis, WorkerDecorator.identity());
    }

    /** Full constructor used by deterministic lifecycle tests. */
    public AnalyticsService(PopularWindowStore windowStore, AnalyticsLog log,
            int initialBacklogThreshold, long shutdownTimeoutMillis,
            WorkerDecorator workerDecorator) {
        this.windowStore = Objects.requireNonNull(windowStore, "windowStore must not be null");
        this.log = Objects.requireNonNull(log, "log must not be null");
        Objects.requireNonNull(workerDecorator, "workerDecorator must not be null");
        if (initialBacklogThreshold <= 0) {
            throw new IllegalArgumentException("initialBacklogThreshold must be positive");
        }
        if (shutdownTimeoutMillis <= 0) {
            throw new IllegalArgumentException("shutdownTimeoutMillis must be positive");
        }
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
        nextIngressBacklogThreshold = initialBacklogThreshold;
        long persistedMaximumEnd = windowStore.readMaxWindowEnd();

        Runnable windowFilter = new WindowFilter(ingressQueue, windowQueue, persistedMaximumEnd);
        Runnable rankingFilter = new RankingFilter(windowQueue, rankingQueue);
        Runnable persistenceFilter = new PersistenceFilter(rankingQueue, windowStore, log, Thread::sleep);
        windowWorker = worker(workerDecorator.decorate(AnalyticsLog.Stage.WINDOW, windowFilter),
                AnalyticsLog.Stage.WINDOW, "analytics-window");
        rankingWorker = worker(workerDecorator.decorate(AnalyticsLog.Stage.RANKING, rankingFilter),
                AnalyticsLog.Stage.RANKING, "analytics-ranking");
        persistenceWorker = worker(workerDecorator.decorate(
                AnalyticsLog.Stage.PERSISTENCE, persistenceFilter),
                AnalyticsLog.Stage.PERSISTENCE, "analytics-persistence");

        synchronized (lifecycleLock) {
            persistenceWorker.start();
            rankingWorker.start();
            windowWorker.start();
            state = State.RUNNING;
        }
    }

    @Override
    public void recordAcceptedScan(String sku) {
        PipelineMessage.AcceptedScan message = new PipelineMessage.AcceptedScan(sku);
        synchronized (lifecycleLock) {
            if (state != State.RUNNING) {
                log.ingestionRejected(state.name(), rejectionReason(state));
                throw new IllegalStateException("analytics pipeline is not accepting scans: " + state);
            }
            if (!ingressQueue.offer(message)) {
                log.ingestionRejected(state.name(), "unbounded analytics ingress rejected a scan");
                throw new IllegalStateException("unbounded analytics ingress rejected a scan");
            }
            recordIngressHighWater();
        }
    }

    @Override
    public PopularItemsView latestPopularItems(int limit) {
        if (limit < 0) throw new IllegalArgumentException("limit must be non-negative");
        return windowStore.readLatest(limit).map(window -> new PopularItemsView(
                WINDOW_SIZE, SLIDE_INTERVAL, window.windowStart(), window.windowEnd(),
                window.computedAt(), window.ranks().stream()
                        .map(rank -> new RankedItemView(rank.sku(), rank.name(), rank.scanCount(), rank.rank()))
                        .toList()))
                .orElseGet(PopularItemsView::empty);
    }

    @Override
    public void shutdown() {
        synchronized (lifecycleLock) {
            if (state == State.TERMINATED || state == State.FORCED) return;
            if (state == State.FAILED) {
                interruptWorkers(Thread.currentThread());
                return;
            }
            if (state == State.DRAINING) return;
            state = State.DRAINING;
            if (!endMarkerSent) {
                ingressQueue.offer(PipelineMessage.IngressEnd.INSTANCE);
                endMarkerSent = true;
            }
            log.drainStarted(ingressQueue.size(), windowQueue.size(), rankingQueue.size());
        }

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMillis);
        boolean callerInterrupted = false;
        for (Thread worker : workers()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            try {
                TimeUnit.NANOSECONDS.timedJoin(worker, remaining);
            } catch (InterruptedException interrupted) {
                callerInterrupted = true;
                break;
            }
        }

        boolean force = callerInterrupted || anyWorkerAlive();
        synchronized (lifecycleLock) {
            if (state == State.FAILED) {
                force = true;
            } else if (force) {
                state = State.FORCED;
                log.shutdownForced(callerInterrupted
                        ? AnalyticsLog.ShutdownOutcome.INTERRUPTED
                        : AnalyticsLog.ShutdownOutcome.TIMEOUT);
            } else {
                state = State.TERMINATED;
                log.shutdownCompleted();
            }
        }
        if (force) interruptWorkers(Thread.currentThread());
        if (callerInterrupted) Thread.currentThread().interrupt();
    }

    public State state() {
        return state;
    }

    private Thread worker(Runnable filter, AnalyticsLog.Stage stage, String name) {
        Runnable requiredFilter = Objects.requireNonNull(filter, "decorated worker must not be null");
        Thread thread = new Thread(() -> {
            try {
                requiredFilter.run();
                if (!Thread.currentThread().isInterrupted()) {
                    failOnNormalExit(stage, new IllegalStateException(
                            stage.name().toLowerCase() + " worker exited without end marker"));
                }
            } catch (Throwable failure) {
                failIfUnexpectedExit(stage, failure);
            }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    private void failOnNormalExit(AnalyticsLog.Stage stage, Throwable failure) {
        boolean unexpected;
        synchronized (lifecycleLock) {
            unexpected = state == State.NEW || state == State.RUNNING;
        }
        if (unexpected) failIfUnexpectedExit(stage, failure);
    }

    private void failIfUnexpectedExit(AnalyticsLog.Stage stage, Throwable failure) {
        boolean firstFailure = false;
        synchronized (lifecycleLock) {
            if (state == State.NEW || state == State.RUNNING || state == State.DRAINING) {
                state = State.FAILED;
                log.workerFailed(stage, failure);
                firstFailure = true;
            }
        }
        if (firstFailure) interruptWorkers(Thread.currentThread());
    }

    private void interruptWorkers(Thread except) {
        for (Thread worker : workers()) {
            if (worker != except && worker != null && worker.isAlive()) worker.interrupt();
        }
    }

    private Thread[] workers() {
        return new Thread[]{windowWorker, rankingWorker, persistenceWorker};
    }

    private boolean anyWorkerAlive() {
        return windowWorker.isAlive() || rankingWorker.isAlive() || persistenceWorker.isAlive();
    }

    private static String rejectionReason(State state) {
        return switch (state) {
            case NEW -> "analytics pipeline has not started";
            case RUNNING -> "analytics pipeline rejected a scan";
            case DRAINING -> "analytics pipeline is draining";
            case TERMINATED -> "analytics pipeline is terminated";
            case FAILED -> "analytics pipeline has failed";
            case FORCED -> "analytics pipeline was forcibly terminated";
        };
    }

    private void recordIngressHighWater() {
        int depth = ingressQueue.size();
        if (depth <= ingressHighWater) return;
        ingressHighWater = depth;
        if (depth < nextIngressBacklogThreshold) return;
        log.backlog(AnalyticsLog.Stage.INGRESS, "ingress", depth);
        do {
            nextIngressBacklogThreshold = nextIngressBacklogThreshold > Long.MAX_VALUE / 2
                    ? Long.MAX_VALUE : nextIngressBacklogThreshold * 2;
        } while (nextIngressBacklogThreshold <= depth
                && nextIngressBacklogThreshold != Long.MAX_VALUE);
    }

    public enum State {
        NEW, RUNNING, DRAINING, TERMINATED, FAILED, FORCED
    }

    @FunctionalInterface
    public interface WorkerDecorator {
        Runnable decorate(AnalyticsLog.Stage stage, Runnable worker);

        static WorkerDecorator identity() {
            return (stage, worker) -> worker;
        }
    }
}
