package database;

import api.json.Json;
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
 * JDBC adapter for {@code checkout_operation} and the stock it changes (R8). One apply is one local
 * transaction at READ COMMITTED, with the same statements whatever the basket size:
 *
 * <ol>
 *   <li>{@code SELECT … FOR UPDATE} locks every referenced stock row in ascending SKU order, so a
 *       concurrent duplicate of the same attempt waits here;</li>
 *   <li>the outcome is decided in memory;</li>
 *   <li>{@code INSERT checkout_operation} records it;</li>
 *   <li>one {@code UPDATE … CASE} deducts every line, only when APPLIED;</li>
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
    private static final String INSERT = "INSERT INTO checkout_operation "
            + "(transaction_id, attempt_number, state, rejection_reason, rejected_sku, `lines`, processed_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String FIND = "SELECT state, rejection_reason, rejected_sku, `lines` FROM checkout_operation "
            + "WHERE transaction_id = ? AND attempt_number = ?";
    /** Signals that the attempt's primary key exists, so the recorded row decides the answer. */
    private static final Object RECORDED = new Object();

    private final ConnectionPool pool;

    public JdbcCheckoutOperationStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public ApplyResult apply(Attempt attempt, List<Line> lines, String canonicalLines) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(canonicalLines, "canonicalLines");
        List<Line> ordered = List.copyOf(lines);
        if (ordered.isEmpty()) throw new IllegalArgumentException("lines must not be empty");
        AtomicInteger runs = new AtomicInteger();
        Object result;
        try {
            result = JdbcUnitOfWork.inContendedTransaction(pool, "inventory.apply", connection -> {
                runs.incrementAndGet();
                return applyOnce(connection, attempt, ordered, canonicalLines);
            });
        } finally {
            if (runs.get() > 1) RETRIES.add(runs.get() - 1);
        }
        if (result != RECORDED) return (ApplyResult) result;
        Recorded recorded = find(attempt).orElseThrow(
                () -> new StoreFailure("Checkout operation " + attempt + " was reported as recorded but is missing"));
        return replay(recorded, canonicalLines);
    }

    private static Object applyOnce(Connection connection, Attempt attempt, List<Line> lines, String canonicalLines)
            throws SQLException {
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

        try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
            insert.setString(1, attempt.transactionId());
            insert.setInt(2, attempt.attemptNumber());
            insert.setString(3, outcome == ApplyOutcome.APPLIED ? "APPLIED" : "REJECTED");
            insert.setString(4, outcome == ApplyOutcome.APPLIED ? null : outcome.name());
            insert.setString(5, rejectedSku);
            insert.setString(6, canonicalLines);
            insert.setTimestamp(7, Timestamp.from(Instant.now()));
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
        return new ApplyResult(outcome, rejectedSku, false);
    }

    /** The recorded row decides a repeated request (FR-028). */
    private static ApplyResult replay(Recorded recorded, String canonicalLines) {
        if (recorded.state() == State.BLOCKED) return new ApplyResult(ApplyOutcome.STOCK_RESTORED, null, true);
        if (!canonicalLines.equals(recorded.lines())) return new ApplyResult(ApplyOutcome.PAYLOAD_MISMATCH, null, true);
        return switch (recorded.state()) {
            case APPLIED -> new ApplyResult(ApplyOutcome.APPLIED, null, true);
            case RESTORED, BLOCKED -> new ApplyResult(ApplyOutcome.STOCK_RESTORED, null, true);
            case REJECTED -> new ApplyResult(ApplyOutcome.valueOf(recorded.rejectionReason()), recorded.rejectedSku(), true);
        };
    }

    @Override
    public Optional<Recorded> find(Attempt attempt) {
        Objects.requireNonNull(attempt, "attempt");
        return JdbcUnitOfWork.read(pool, "inventory.outcome", connection -> {
            try (PreparedStatement select = connection.prepareStatement(FIND)) {
                select.setString(1, attempt.transactionId());
                select.setInt(2, attempt.attemptNumber());
                try (ResultSet row = select.executeQuery()) {
                    if (!row.next()) return Optional.empty();
                    return Optional.of(new Recorded(State.valueOf(row.getString(1)), row.getString(2), row.getString(3),
                            row.getString(4)));
                }
            }
        });
    }

    /**
     * One transaction that locks the operation row first. An absent row gets a BLOCKED tombstone, so any
     * later apply of the attempt is refused; a concurrent first apply or restore that wins the insert
     * makes this unit start over.
     */
    @Override
    public RestoreResult restore(Attempt attempt) {
        Objects.requireNonNull(attempt, "attempt");
        for (int run = 1; run <= RESTORE_ATTEMPTS; run++) {
            Object result = JdbcUnitOfWork.inContendedTransaction(pool, "inventory.restore",
                    connection -> restoreOnce(connection, attempt));
            if (result != RECORDED) return (RestoreResult) result;
        }
        throw StoreFailure.contended("Checkout operation " + attempt + " kept changing during restoration", null);
    }

    private static Object restoreOnce(Connection connection, Attempt attempt) throws SQLException {
        State state;
        String lines;
        try (PreparedStatement select = connection.prepareStatement(FIND + " FOR UPDATE")) {
            select.setString(1, attempt.transactionId());
            select.setInt(2, attempt.attemptNumber());
            try (ResultSet row = select.executeQuery()) {
                if (!row.next()) {
                    state = null;
                    lines = null;
                } else {
                    state = State.valueOf(row.getString(1));
                    lines = row.getString(4);
                }
            }
        }
        if (state == null) {
            try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
                insert.setString(1, attempt.transactionId());
                insert.setInt(2, attempt.attemptNumber());
                insert.setString(3, "BLOCKED");
                insert.setString(4, null);
                insert.setString(5, null);
                insert.setString(6, null);
                insert.setTimestamp(7, Timestamp.from(Instant.now()));
                insert.executeUpdate();
            } catch (SQLException failure) {
                if (failure.getErrorCode() == DUPLICATE_KEY) throw JdbcUnitOfWork.rollbackAndReturn(RECORDED);
                throw failure;
            }
            return new RestoreResult(RestoreOutcome.NOTHING_TO_RESTORE, null);
        }
        return switch (state) {
            case RESTORED -> new RestoreResult(RestoreOutcome.ALREADY_RESTORED, lines);
            case REJECTED, BLOCKED -> new RestoreResult(RestoreOutcome.NOTHING_TO_RESTORE, null);
            case APPLIED -> {
                List<Line> restored = parse(lines);
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
                        + "restored_at = ? WHERE transaction_id = ? AND attempt_number = ? AND state = 'APPLIED'")) {
                    mark.setTimestamp(1, Timestamp.from(Instant.now()));
                    mark.setString(2, attempt.transactionId());
                    mark.setInt(3, attempt.attemptNumber());
                    if (mark.executeUpdate() != 1) throw new SQLException("The locked operation row changed");
                }
                yield new RestoreResult(RestoreOutcome.RESTORED, lines);
            }
        };
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
