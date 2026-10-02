package support;

import database.PopularWindowStore;
import database.StoreFailure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** Shared deterministic fakes and synchronization helpers for analytics pipeline tests. */
public final class PipelineTestSupport {
    public static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(5);

    private PipelineTestSupport() {
    }

    /** Polls without an arbitrary fixed sleep and returns false when the deadline expires. */
    public static boolean await(BooleanSupplier condition) throws InterruptedException {
        return await(DEFAULT_AWAIT_TIMEOUT, condition);
    }

    /** Polls until {@code condition} is true or the supplied timeout expires. */
    public static boolean await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout must not be null");
        Objects.requireNonNull(condition, "condition must not be null");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }

        long deadline = System.nanoTime() + timeout.toNanos();
        do {
            if (condition.getAsBoolean()) {
                return true;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return condition.getAsBoolean();
            }
            long sleepNanos = Math.min(DEFAULT_POLL_INTERVAL.toNanos(), remaining);
            TimeUnit.NANOSECONDS.sleep(sleepNanos);
        } while (true);
    }

    /** Fails with a useful message instead of leaving each test to implement a spin loop. */
    public static void awaitOrFail(String description, BooleanSupplier condition)
            throws InterruptedException {
        awaitOrFail(DEFAULT_AWAIT_TIMEOUT, description, condition);
    }

    public static void awaitOrFail(Duration timeout, String description, BooleanSupplier condition)
            throws InterruptedException {
        if (!await(timeout, condition)) {
            throw new AssertionError("Timed out waiting for " + Objects.requireNonNull(description));
        }
    }

    /**
     * A resettable gate that can pass, fail, fail a fixed number of operations, or hold an
     * operation until a test releases it. The wait is interruptible so shutdown tests cannot hang.
     */
    public static final class FailureGate {
        private boolean blocked;
        private boolean failContinuously;
        private int failuresRemaining;

        public synchronized void pass() {
            blocked = false;
            failContinuously = false;
            failuresRemaining = 0;
            notifyAll();
        }

        public synchronized void fail() {
            blocked = false;
            failContinuously = true;
            failuresRemaining = 0;
            notifyAll();
        }

        public synchronized void failNext(int operationCount) {
            if (operationCount < 0) {
                throw new IllegalArgumentException("operationCount must not be negative");
            }
            blocked = false;
            failContinuously = false;
            failuresRemaining = operationCount;
            notifyAll();
        }

        public synchronized void block() {
            blocked = true;
        }

        public synchronized boolean isBlocked() {
            return blocked;
        }

        public synchronized void beforeOperation() {
            while (blocked) {
                try {
                    wait();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StoreFailure("injected operation interrupted", interrupted);
                }
            }
            if (failContinuously || failuresRemaining > 0) {
                if (failuresRemaining > 0) {
                    failuresRemaining--;
                }
                throw new StoreFailure("injected persistence failure");
            }
        }
    }

    /** In-memory store with deterministic recovery, failure, attempt, commit, and read behavior. */
    public static final class FakePopularWindowStore implements PopularWindowStore {
        private final AtomicLong recoveredWindowEnd;
        private final FailureGate writeGate = new FailureGate();
        private final AtomicInteger writeAttempts = new AtomicInteger();
        private final List<PopularWindowSnapshot> attemptedWindows =
                Collections.synchronizedList(new ArrayList<>());
        private final List<PopularWindowSnapshot> committedWindows =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Optional<PopularWindowView>> latest =
                new AtomicReference<>(Optional.empty());
        private volatile int lastReadLimit = -1;

        public FakePopularWindowStore() {
            this(0);
        }

        public FakePopularWindowStore(long recoveredWindowEnd) {
            if (recoveredWindowEnd < 0) {
                throw new IllegalArgumentException("recoveredWindowEnd must not be negative");
            }
            this.recoveredWindowEnd = new AtomicLong(recoveredWindowEnd);
        }

        @Override
        public long readMaxWindowEnd() {
            return recoveredWindowEnd.get();
        }

        @Override
        public void writeWindow(PopularWindowSnapshot snapshot) {
            PopularWindowSnapshot required = Objects.requireNonNull(snapshot, "snapshot must not be null");
            attemptedWindows.add(required);
            writeAttempts.incrementAndGet();
            writeGate.beforeOperation();
            committedWindows.add(required);
            recoveredWindowEnd.accumulateAndGet(required.windowEnd(), Math::max);
        }

        @Override
        public Optional<PopularWindowView> readLatest(int limit) {
            lastReadLimit = limit;
            return latest.get().map(view -> new PopularWindowView(
                    view.windowId(), view.windowStart(), view.windowEnd(), view.computedAt(),
                    view.ranks().subList(0, Math.min(limit, view.ranks().size()))));
        }

        public FailureGate writeGate() {
            return writeGate;
        }

        public int writeAttemptCount() {
            return writeAttempts.get();
        }

        public int lastReadLimit() {
            return lastReadLimit;
        }

        public List<PopularWindowSnapshot> attemptedWindows() {
            synchronized (attemptedWindows) {
                return List.copyOf(attemptedWindows);
            }
        }

        public List<PopularWindowSnapshot> committedWindows() {
            synchronized (committedWindows) {
                return List.copyOf(committedWindows);
            }
        }

        public void setLatest(PopularWindowView view) {
            latest.set(Optional.of(Objects.requireNonNull(view, "view must not be null")));
        }

        public void clearLatest() {
            latest.set(Optional.empty());
        }

        public boolean awaitAttempts(int count) throws InterruptedException {
            return await(() -> writeAttemptCount() >= count);
        }

        public boolean awaitCommits(int count) throws InterruptedException {
            return await(() -> committedWindows.size() >= count);
        }
    }

    /** Thread-safe collector suitable for an injectable one-JSON-object-per-call log sink. */
    public static final class OperationalLogCollector implements Consumer<String> {
        private final List<String> records = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void accept(String record) {
            String required = Objects.requireNonNull(record, "record must not be null");
            if (required.indexOf('\n') >= 0 || required.indexOf('\r') >= 0) {
                throw new IllegalArgumentException("an operational log record must be one line");
            }
            records.add(required);
        }

        public List<String> records() {
            synchronized (records) {
                return List.copyOf(records);
            }
        }

        public long count(Predicate<String> predicate) {
            Objects.requireNonNull(predicate, "predicate must not be null");
            synchronized (records) {
                return records.stream().filter(predicate).count();
            }
        }

        public boolean awaitRecord(Predicate<String> predicate) throws InterruptedException {
            return await(() -> count(predicate) > 0);
        }
    }
}
