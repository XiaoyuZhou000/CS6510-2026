package unit.transaction;

import database.CheckoutAttemptStore;
import database.StoreFailure;
import database.TransactionStore;
import support.InMemoryOutboxStore;
import transaction.TransactionOperations;
import transaction.TransactionStateCache;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Transaction rows, basket lines, attempts, and the {@code CheckoutRequested} outbox rows T1 records, behind
 * the same guards as the real stores, for the unit tests of the checkout Saga's Transaction side. T1 adds its
 * request row to {@link #outbox} exactly when it commits (FR-010).
 */
final class CheckoutDb implements TransactionStore, CheckoutAttemptStore {
    record Row(String stationId, TransactionStatus status, int itemCount, BigDecimal total, int attemptCount,
               Integer active, Instant startedAt, Instant completedAt) { }

    /** A scripted failure, raised before or after the write took effect. */
    record Failure(StoreFailure failure, boolean afterCommit) { }

    final Instant base;
    final InMemoryOutboxStore outbox = new InMemoryOutboxStore();
    final Map<String, Row> rows = new HashMap<>();
    final Map<String, List<TransactionLine>> lines = new HashMap<>();
    final Map<AttemptKey, AttemptRecord> attempts = new HashMap<>();
    final List<String> calls = new ArrayList<>();
    final List<Thread> findThreads = new ArrayList<>();
    final Deque<Failure> completeFailures = new ArrayDeque<>();
    final Deque<Failure> reopenFailures = new ArrayDeque<>();
    Failure beginFailure;
    StoreFailure pendingErrorFailure;
    StoreFailure clientOutcomeFailure;
    StoreFailure findFailure;
    /** Runs at the start of every T3 and reopen with the transaction ID, for example to check the stripe. */
    java.util.function.Consumer<String> onSettle = id -> { };
    int loads;
    int finds;
    int guardMisses;
    int clientOutcomeWrites;

    CheckoutDb(Instant base) {
        this.base = base;
    }

    synchronized void open(String id, TransactionLine... basket) {
        int units = 0;
        BigDecimal total = new BigDecimal("0.00");
        for (TransactionLine line : basket) {
            units += line.quantity();
            total = total.add(line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())));
        }
        rows.put(id, new Row("station", TransactionStatus.OPEN, units, total, 0, null, base.minusSeconds(60), null));
        lines.put(id, new ArrayList<>(List.of(basket)));
    }

    synchronized Row row(String id) {
        return rows.get(id);
    }

    synchronized AttemptRecord attempt(String id, int number) {
        return attempts.get(new AttemptKey(id, number));
    }

    synchronized List<AttemptRecord> attempts(String id) {
        return attempts.values().stream().filter(a -> a.key().transactionId().equals(id)).toList();
    }

    synchronized int findsBy(Thread thread) {
        return (int) findThreads.stream().filter(thread::equals).count();
    }

    synchronized void completeBehindTheCache(String id) {
        Row r = rows.get(id);
        rows.put(id, new Row(r.stationId(), TransactionStatus.COMPLETED, r.itemCount(), r.total(), r.attemptCount(),
                null, r.startedAt(), base));
    }

    /** Another instance's T1: the row's active attempt and its PENDING attempt row. */
    synchronized AttemptKey beginBehindTheCache(String id) {
        Row r = rows.get(id);
        int number = r.attemptCount() + 1;
        rows.put(id, new Row(r.stationId(), r.status(), r.itemCount(), r.total(), number, number, r.startedAt(), null));
        AttemptKey key = new AttemptKey(id, number);
        attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, "[]", r.itemCount(), r.total(),
                ClientOutcome.NONE, base, null, 0, base.plusSeconds(2), null, "request-" + key));
        return key;
    }

    synchronized void foreignScan(String id, TransactionLine line) {
        Row r = rows.get(id);
        lines.get(id).add(line);
        rows.put(id, new Row(r.stationId(), r.status(), r.itemCount() + line.quantity(),
                r.total().add(line.unitPrice().multiply(BigDecimal.valueOf(line.quantity()))), r.attemptCount(),
                r.active(), r.startedAt(), r.completedAt()));
    }

    /** The service's own cold-path mapping, used to warm the cache as a Scan would leave it. */
    Optional<TransactionStateCache.State> loadStateAsCache(String id) {
        return loadState(id).map(state -> {
            Map<String, TransactionStateCache.Line> basket = new java.util.TreeMap<>();
            for (TransactionLine line : state.lines()) {
                basket.put(line.sku(), new TransactionStateCache.Line(line.sku(), line.name(), line.unitPrice(),
                        line.quantity()));
            }
            var row = state.transaction();
            return new TransactionStateCache.State(id, TransactionOperations.TransactionStatus.valueOf(row.status().name()),
                    row.itemCount(), row.totalAmount(), state.attemptCount(), state.activeAttemptNumber(), basket,
                    row.stationId(), row.startedAt());
        });
    }

    @Override
    public void insertOpen(String transactionId, String stationId, Instant startedAt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Optional<TransactionRecord> findById(String id) {
        Row r = rows.get(id);
        return r == null ? Optional.empty() : Optional.of(record(id, r));
    }

    @Override
    public synchronized Optional<TransactionState> loadState(String id) {
        loads++;
        Row r = rows.get(id);
        if (r == null) return Optional.empty();
        return Optional.of(new TransactionState(record(id, r), r.attemptCount(), r.active(), lines.get(id)));
    }

    private TransactionRecord record(String id, Row r) {
        return new TransactionRecord(id, r.stationId(), r.status(), r.total(), r.startedAt(), r.completedAt(),
                r.itemCount());
    }

    @Override
    public synchronized WriteResult begin(Begin begin) {
        AttemptKey key = begin.key();
        Failure failure = beginFailure;
        beginFailure = null;
        if (failure != null && !failure.afterCommit()) throw failure.failure();
        Row r = rows.get(key.transactionId());
        if (r == null || r.status() != TransactionStatus.OPEN || r.active() != null || r.itemCount() != begin.itemCount()
                || r.attemptCount() != key.attemptNumber() - 1) {
            guardMisses++;
            return WriteResult.GUARD_MISS;
        }
        rows.put(key.transactionId(), new Row(r.stationId(), r.status(), r.itemCount(), r.total(), key.attemptNumber(),
                key.attemptNumber(), r.startedAt(), null));
        attempts.put(key, new AttemptRecord(key, State.PENDING, null, null, begin.lines(), begin.itemCount(),
                begin.totalAmount(), ClientOutcome.NONE, begin.createdAt(), null, 0, begin.nextRetryAt(), null,
                begin.request().eventId()));
        outbox.add(begin.request());
        calls.add("begin " + key);
        if (failure != null) throw failure.failure();
        return WriteResult.OK;
    }

    @Override
    public synchronized WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
        onSettle.accept(key.transactionId());
        Failure failure = completeFailures.poll();
        if (failure != null && !failure.afterCommit()) throw failure.failure();
        Row r = rows.get(key.transactionId());
        if (r == null || !Integer.valueOf(key.attemptNumber()).equals(r.active())) {
            guardMisses++;
            return WriteResult.GUARD_MISS;
        }
        rows.put(key.transactionId(), new Row(r.stationId(), TransactionStatus.COMPLETED, r.itemCount(), r.total(),
                r.attemptCount(), null, r.startedAt(), completedAt));
        settle(key, State.COMPLETED, null, null, completedAt, answering ? ClientOutcome.SUCCESS : null);
        calls.add("complete " + key);
        if (failure != null) throw failure.failure();
        return WriteResult.OK;
    }

    @Override
    public synchronized WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku,
                                           Instant settledAt, boolean answering) {
        onSettle.accept(key.transactionId());
        Failure failure = reopenFailures.poll();
        if (failure != null && !failure.afterCommit()) throw failure.failure();
        Row r = rows.get(key.transactionId());
        if (r == null || !Integer.valueOf(key.attemptNumber()).equals(r.active())) {
            guardMisses++;
            return WriteResult.GUARD_MISS;
        }
        rows.put(key.transactionId(), new Row(r.stationId(), r.status(), r.itemCount(), r.total(), r.attemptCount(),
                null, r.startedAt(), null));
        settle(key, State.REJECTED, reason, rejectedSku, settledAt, answering ? ClientOutcome.REJECTED : null);
        calls.add("reopen " + key);
        if (failure != null) throw failure.failure();
        return WriteResult.OK;
    }

    private void settle(AttemptKey key, State state, RejectionReason reason, String sku, Instant at,
                        ClientOutcome answer) {
        AttemptRecord a = attempts.get(key);
        ClientOutcome outcome = answer != null && a.clientOutcome() == ClientOutcome.NONE ? answer : a.clientOutcome();
        attempts.put(key, new AttemptRecord(key, state, reason, sku, a.lines(), a.itemCount(), a.totalAmount(),
                outcome, a.createdAt(), at, a.retryCount(), a.nextRetryAt(), a.lastError(), a.requestEventId()));
    }

    @Override
    public synchronized boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) {
        if (clientOutcomeFailure != null) throw clientOutcomeFailure;
        AttemptRecord a = attempts.get(key);
        if (a == null || a.clientOutcome() != ClientOutcome.NONE) return false;
        clientOutcomeWrites++;
        attempts.put(key, new AttemptRecord(key, a.state(), a.rejectionReason(), a.rejectedSku(), a.lines(),
                a.itemCount(), a.totalAmount(), outcome, a.createdAt(), a.settledAt(), a.retryCount(),
                a.nextRetryAt(), a.lastError(), a.requestEventId()));
        calls.add("recordClientOutcome " + key);
        return true;
    }

    @Override
    public synchronized boolean markPendingError(AttemptKey key, Instant now) {
        if (pendingErrorFailure != null) throw pendingErrorFailure;
        AttemptRecord a = attempts.get(key);
        if (a == null || a.state() != State.PENDING || a.clientOutcome() != ClientOutcome.NONE) return false;
        attempts.put(key, new AttemptRecord(key, a.state(), null, null, a.lines(), a.itemCount(), a.totalAmount(),
                ClientOutcome.PENDING_ERROR, a.createdAt(), null, a.retryCount(), now, a.lastError(), a.requestEventId()));
        calls.add("markPendingError " + key);
        return true;
    }

    @Override
    public List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) {
        throw new UnsupportedOperationException("recovery is not part of this test");
    }

    @Override
    public synchronized Optional<AttemptRecord> findAttempt(AttemptKey key) {
        if (findFailure != null) throw findFailure;
        finds++;
        findThreads.add(Thread.currentThread());
        return Optional.ofNullable(attempts.get(key));
    }

    @Override
    public synchronized void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) {
        AttemptRecord a = attempts.get(key);
        if (a == null || a.state() != State.PENDING) return;
        String error = a.lastError() != null && a.lastError().startsWith("INTEGRITY:") ? a.lastError() : lastError;
        attempts.put(key, new AttemptRecord(key, a.state(), a.rejectionReason(), a.rejectedSku(), a.lines(),
                a.itemCount(), a.totalAmount(), a.clientOutcome(), a.createdAt(), a.settledAt(), a.retryCount() + 1,
                nextRetryAt, error, a.requestEventId()));
        calls.add("scheduleRetry " + key + " " + error);
    }

    @Override
    public synchronized boolean recordError(AttemptKey key, String lastError) {
        AttemptRecord a = attempts.get(key);
        if (a == null) return false;
        attempts.put(key, new AttemptRecord(key, a.state(), a.rejectionReason(), a.rejectedSku(), a.lines(),
                a.itemCount(), a.totalAmount(), a.clientOutcome(), a.createdAt(), a.settledAt(), a.retryCount(),
                a.nextRetryAt(), lastError, a.requestEventId()));
        calls.add("recordError " + key + " " + lastError);
        return true;
    }

    static TransactionLine line(String sku, String name, String price, int quantity) {
        return new TransactionLine(sku, name, quantity, new BigDecimal(price));
    }
}
