package database;

import api.json.Json;
import events.OutboxRow;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * JDBC adapter for {@code checkout_operation}, the stock it changes, and the outcome events recorded with
 * it (R8, 006 D8). One apply is one local transaction at READ COMMITTED, with the same statements whatever
 * the basket size:
 *
 * <ol>
 *   <li>{@code SELECT … FOR UPDATE} locks every referenced stock row in ascending SKU order, so a
 *       concurrent duplicate of the same attempt waits here;</li>
 *   <li>the outcome is decided in memory;</li>
 *   <li>{@code INSERT checkout_operation} records it with its {@code outcome_event_id};</li>
 *   <li>one {@code UPDATE … CASE} deducts every line, only when APPLIED;</li>
 *   <li>{@code INSERT outbox_event} records the outcome event;</li>
 *   <li>commit.</li>
 * </ol>
 *
 * A duplicate primary key means the attempt is recorded already: the unit rolls back and the recorded
 * row decides the answer. Deadlocks and lock-wait timeouts retry the whole unit.
 */
public final class JdbcCheckoutOperationStore implements CheckoutOperationStore {
    private static final int DUPLICATE_KEY = 1062;
    private static final String APPLIED_INDEX = "uq_checkout_operation_applied";
    private static final int RESTORE_ATTEMPTS = 3;
    private static final LatencyHistogram LOCK_WAIT = PerfMetrics.timer("inventory.apply.lockWait");
    private static final LongAdder RETRIES = PerfMetrics.counter("inventory.apply.retries");
    private static final String INSERT = "INSERT INTO checkout_operation (transaction_id, attempt_number, state, "
            + "rejection_reason, rejected_sku, `lines`, processed_at, outcome_event_id, restoration_event_id) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String FIND = "SELECT state, rejection_reason, rejected_sku, `lines`, outcome_event_id, "
            + "restoration_event_id FROM checkout_operation WHERE transaction_id = ? AND attempt_number = ?";
    /** Signals that the attempt's primary key exists, so the recorded row decides the answer. */
    private static final Object RECORDED = new Object();

    private final ConnectionPool pool;

    public JdbcCheckoutOperationStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public ApplyResult apply(Attempt attempt, List<Line> lines, String canonicalLines, OutcomeEvents outcomes) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(canonicalLines, "canonicalLines");
        Objects.requireNonNull(outcomes, "outcomes");
        List<Line> ordered = List.copyOf(lines);
        if (ordered.isEmpty()) throw new IllegalArgumentException("lines must not be empty");
        AtomicInteger runs = new AtomicInteger();
        Object result;
        try {
            result = JdbcUnitOfWork.inContendedTransaction(pool, "inventory.apply", connection -> {
                runs.incrementAndGet();
                return applyOnce(connection, attempt, ordered, canonicalLines, outcomes);
            });
        } finally {
            if (runs.get() > 1) RETRIES.add(runs.get() - 1);
        }
        if (result != RECORDED) return (ApplyResult) result;
        Recorded recorded = find(attempt).orElseThrow(
                () -> new StoreFailure("Checkout operation " + attempt + " was reported as recorded but is missing"));
        return replay(recorded, canonicalLines);
    }

    private static Object applyOnce(Connection connection, Attempt attempt, List<Line> lines, String canonicalLines,
                                    OutcomeEvents outcomes) throws SQLException {
        Map<String, Integer> stock = new HashMap<>();
        long locking = System.nanoTime();
        try (PreparedStatement select = connection.prepareStatement("SELECT sku, stock_quantity FROM inventory WHERE sku IN ("
                + placeholders(lines.size()) + ") ORDER BY sku FOR UPDATE")) {
            for (int i = 0; i < lines.size(); i++) select.setString(i + 1, lines.get(i).sku());
            try (ResultSet rows = select.executeQuery()) {
                while (rows.next()) stock.put(rows.getString(1), rows.getInt(2));
            }
        } finally {
            LOCK_WAIT.record(System.nanoTime() - locking);
        }

        ApplyOutcome outcome = ApplyOutcome.APPLIED;
        String rejectedSku = null;
        for (Line line : lines) {
            if (!stock.containsKey(line.sku())) {
                outcome = ApplyOutcome.INVALID_REQUEST;
                rejectedSku = line.sku();
                break;
            }
        }
        if (outcome == ApplyOutcome.APPLIED) {
            for (Line line : lines) {
                if (stock.get(line.sku()) < line.quantity()) {
                    outcome = ApplyOutcome.INSUFFICIENT_STOCK;
                    rejectedSku = line.sku();
                    break;
                }
            }
        }
        OutboxRow event = outcomes.outcome(outcome, rejectedSku);

        try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
            insert.setString(1, attempt.transactionId());
            insert.setInt(2, attempt.attemptNumber());
            insert.setString(3, outcome == ApplyOutcome.APPLIED ? "APPLIED" : "REJECTED");
            insert.setString(4, outcome == ApplyOutcome.APPLIED ? null : outcome.name());
            insert.setString(5, rejectedSku);
            insert.setString(6, canonicalLines);
            insert.setTimestamp(7, Timestamp.from(Instant.now()));
            insert.setString(8, event.eventId());
            insert.setString(9, null);
            insert.executeUpdate();
        } catch (SQLException failure) {
            if (failure.getErrorCode() != DUPLICATE_KEY) throw failure;
            if (String.valueOf(failure.getMessage()).contains(APPLIED_INDEX)) {
                throw new IntegrityFailure("Transaction " + attempt.transactionId()
                        + " already has an applied checkout operation", failure);
            }
            throw JdbcUnitOfWork.rollbackAndReturn(RECORDED);
        }

        if (outcome == ApplyOutcome.APPLIED) {
            StringBuilder sql = new StringBuilder("UPDATE inventory SET stock_quantity = CASE sku");
            for (int i = 0; i < lines.size(); i++) sql.append(" WHEN ? THEN stock_quantity - ?");
            sql.append(" END WHERE sku IN (").append(placeholders(lines.size())).append(')');
            try (PreparedStatement update = connection.prepareStatement(sql.toString())) {
                int index = 1;
                for (Line line : lines) {
                    update.setString(index++, line.sku());
                    update.setInt(index++, line.quantity());
                }
                for (Line line : lines) update.setString(index++, line.sku());
                if (update.executeUpdate() != lines.size()) {
                    throw new SQLException("The locked stock rows changed before the deduction");
                }
            }
        }
        JdbcOutboxStore.insertRow(connection, event);
        return new ApplyResult(outcome, rejectedSku, false, event.eventId());
    }

    /** The recorded row decides a repeated request (FR-028), with its recorded outcome event. */
    private static ApplyResult replay(Recorded recorded, String canonicalLines) {
        String eventId = recorded.outcomeEventId();
        if (recorded.state() == State.BLOCKED) return new ApplyResult(ApplyOutcome.STOCK_RESTORED, null, true, eventId);
        if (!canonicalLines.equals(recorded.lines())) {
            return new ApplyResult(ApplyOutcome.PAYLOAD_MISMATCH, null, true, eventId);
        }
        return switch (recorded.state()) {
            case APPLIED -> new ApplyResult(ApplyOutcome.APPLIED, null, true, eventId);
            case RESTORED, BLOCKED -> new ApplyResult(ApplyOutcome.STOCK_RESTORED, null, true, eventId);
            case REJECTED -> new ApplyResult(ApplyOutcome.valueOf(recorded.rejectionReason()), recorded.rejectedSku(),
                    true, eventId);
        };
    }

    @Override
    public boolean recordRefusal(OutboxRow refusal) {
        Objects.requireNonNull(refusal, "refusal");
        return JdbcUnitOfWork.inTransaction(pool, "inventory.refusal", connection -> {
            try {
                JdbcOutboxStore.insertRow(connection, refusal);
            } catch (SQLException failure) {
                if (failure.getErrorCode() == DUPLICATE_KEY) throw JdbcUnitOfWork.rollbackAndReturn(false);
                throw failure;
            }
            return true;
        });
    }

    @Override
    public Optional<Recorded> find(Attempt attempt) {
        Objects.requireNonNull(attempt, "attempt");
        return JdbcUnitOfWork.read(pool, "inventory.outcome", connection -> {
            try (PreparedStatement select = connection.prepareStatement(FIND)) {
                select.setString(1, attempt.transactionId());
                select.setInt(2, attempt.attemptNumber());
                try (ResultSet row = select.executeQuery()) {
                    return row.next() ? Optional.of(recorded(row)) : Optional.empty();
                }
            }
        });
    }

    private static Recorded recorded(ResultSet row) throws SQLException {
        return new Recorded(State.valueOf(row.getString(1)), row.getString(2), row.getString(3), row.getString(4),
                row.getString(5), row.getString(6));
    }

    /**
     * One transaction that locks the operation row first. An absent row gets a BLOCKED tombstone, so any
     * later apply of the attempt is refused; a concurrent first apply or restore that wins the insert
     * makes this unit start over. A restoration whose event is the row's recorded {@code restoration_event_id},
     * or is in the outbox already, is a redelivery: it rolls back and returns the recorded event.
     */
    @Override
    public RestoreResult restore(Attempt attempt, RestorationEvents events) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(events, "events");
        for (int run = 1; run <= RESTORE_ATTEMPTS; run++) {
            Object result = JdbcUnitOfWork.inContendedTransaction(pool, "inventory.restore",
                    connection -> restoreOnce(connection, attempt, events));
            if (result != RECORDED) return (RestoreResult) result;
        }
        throw StoreFailure.contended("Checkout operation " + attempt + " kept changing during restoration", null);
    }

    private static Object restoreOnce(Connection connection, Attempt attempt, RestorationEvents events)
            throws SQLException {
        Recorded recorded;
        try (PreparedStatement select = connection.prepareStatement(FIND + " FOR UPDATE")) {
            select.setString(1, attempt.transactionId());
            select.setInt(2, attempt.attemptNumber());
            try (ResultSet row = select.executeQuery()) {
                recorded = row.next() ? recorded(row) : null;
            }
        }
        if (recorded == null) {
            OutboxRow event = events.restoration(RestoreOutcome.NOTHING_TO_RESTORE);
            try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
                insert.setString(1, attempt.transactionId());
                insert.setInt(2, attempt.attemptNumber());
                insert.setString(3, "BLOCKED");
                insert.setString(4, null);
                insert.setString(5, null);
                insert.setString(6, null);
                insert.setTimestamp(7, Timestamp.from(Instant.now()));
                insert.setString(8, event.eventId());
                insert.setString(9, event.eventId());
                insert.executeUpdate();
            } catch (SQLException failure) {
                if (failure.getErrorCode() == DUPLICATE_KEY) throw JdbcUnitOfWork.rollbackAndReturn(RECORDED);
                throw failure;
            }
            insertEvent(connection, event, RestoreOutcome.NOTHING_TO_RESTORE, null);
            return new RestoreResult(RestoreOutcome.NOTHING_TO_RESTORE, null, event.eventId(), false);
        }
        RestoreOutcome outcome = switch (recorded.state()) {
            case RESTORED -> RestoreOutcome.ALREADY_RESTORED;
            case REJECTED, BLOCKED -> RestoreOutcome.NOTHING_TO_RESTORE;
            case APPLIED -> RestoreOutcome.RESTORED;
        };
        OutboxRow event = events.restoration(outcome);
        if (event.eventId().equals(recorded.restorationEventId())) {
            RestoreOutcome first = recorded.state() == State.RESTORED ? RestoreOutcome.RESTORED : RestoreOutcome.NOTHING_TO_RESTORE;
            throw JdbcUnitOfWork.rollbackAndReturn(new RestoreResult(first, recorded.lines(), event.eventId(), true));
        }
        if (outcome == RestoreOutcome.RESTORED) {
            List<Line> restored = parse(recorded.lines());
            StringBuilder sql = new StringBuilder("UPDATE inventory SET stock_quantity = CASE sku");
            for (int i = 0; i < restored.size(); i++) sql.append(" WHEN ? THEN stock_quantity + ?");
            sql.append(" END WHERE sku IN (").append(placeholders(restored.size())).append(')');
            try (PreparedStatement update = connection.prepareStatement(sql.toString())) {
                int index = 1;
                for (Line line : restored) {
                    update.setString(index++, line.sku());
                    update.setInt(index++, line.quantity());
                }
                for (Line line : restored) update.setString(index++, line.sku());
                update.executeUpdate();
            }
            try (PreparedStatement mark = connection.prepareStatement("UPDATE checkout_operation SET state = 'RESTORED', "
                    + "restored_at = ?, restoration_event_id = ? WHERE transaction_id = ? AND attempt_number = ? "
                    + "AND state = 'APPLIED'")) {
                mark.setTimestamp(1, Timestamp.from(Instant.now()));
                mark.setString(2, event.eventId());
                mark.setString(3, attempt.transactionId());
                mark.setInt(4, attempt.attemptNumber());
                if (mark.executeUpdate() != 1) throw new SQLException("The locked operation row changed");
            }
        } else if (recorded.restorationEventId() == null) {
            try (PreparedStatement mark = connection.prepareStatement("UPDATE checkout_operation SET "
                    + "restoration_event_id = ? WHERE transaction_id = ? AND attempt_number = ?")) {
                mark.setString(1, event.eventId());
                mark.setString(2, attempt.transactionId());
                mark.setInt(3, attempt.attemptNumber());
                mark.executeUpdate();
            }
        }
        String lines = outcome == RestoreOutcome.NOTHING_TO_RESTORE ? null : recorded.lines();
        insertEvent(connection, event, outcome, lines);
        return new RestoreResult(outcome, lines, event.eventId(), false);
    }

    /** Inserts a restoration's event; one already recorded makes the whole unit a replay of it. */
    private static void insertEvent(Connection connection, OutboxRow event, RestoreOutcome outcome, String lines)
            throws SQLException {
        try {
            JdbcOutboxStore.insertRow(connection, event);
        } catch (SQLException failure) {
            if (failure.getErrorCode() != DUPLICATE_KEY) throw failure;
            throw JdbcUnitOfWork.rollbackAndReturn(new RestoreResult(outcome, lines, event.eventId(), true));
        }
    }

    /** Reads a stored canonical payload, {@code [{"sku":"…","quantity":n},…]}. */
    private static List<Line> parse(String canonical) {
        List<Line> lines = new ArrayList<>();
        Object parsed = Json.parse(canonical);
        if (!(parsed instanceof List<?> entries)) throw new StoreFailure("A stored checkout payload is not a JSON array");
        for (Object entry : entries) {
            Map<String, Object> line = Json.asObject(entry);
            lines.add(new Line(Json.getString(line, "sku"), Json.getInt(line, "quantity")));
        }
        return lines;
    }

    private static String placeholders(int count) {
        return String.join(", ", java.util.Collections.nCopies(count, "?"));
    }
}
