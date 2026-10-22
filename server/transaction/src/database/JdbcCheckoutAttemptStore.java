package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC adapter for {@code checkout_attempt} and the Saga's guarded writes to {@code transaction}
 * (contracts/checkout-saga.md "Steps"). T1, T3, and reopen are two statements and one commit each,
 * with no read; every timestamp is supplied by the service.
 */
public final class JdbcCheckoutAttemptStore implements CheckoutAttemptStore {
    private static final int LAST_ERROR_LENGTH = 255;
    private static final String BEGIN_GUARD = "UPDATE `transaction` SET attempt_count = ?, active_attempt_number = ? "
            + "WHERE transaction_id = ? AND status = 'OPEN' AND active_attempt_number IS NULL AND item_count = ? "
            + "AND attempt_count = ?";
    private static final String BEGIN_INSERT = "INSERT INTO checkout_attempt (transaction_id, attempt_number, state, "
            + "`lines`, item_count, total_amount, client_outcome, created_at, next_retry_at) "
            + "VALUES (?, ?, 'PENDING', ?, ?, ?, 'NONE', ?, ?)";
    private static final String COMPLETE_GUARD = "UPDATE `transaction` SET status = 'COMPLETED', completed_at = ?, "
            + "active_attempt_number = NULL WHERE transaction_id = ? AND active_attempt_number = ?";
    private static final String COMPLETE_ATTEMPT = "UPDATE checkout_attempt SET state = 'COMPLETED', settled_at = ?, "
            + "client_outcome = IF(? AND client_outcome = 'NONE', 'SUCCESS', client_outcome) "
            + "WHERE transaction_id = ? AND attempt_number = ? AND state = 'PENDING'";
    private static final String REOPEN_GUARD = "UPDATE `transaction` SET active_attempt_number = NULL "
            + "WHERE transaction_id = ? AND active_attempt_number = ?";
    private static final String REOPEN_ATTEMPT = "UPDATE checkout_attempt SET state = 'REJECTED', rejection_reason = ?, "
            + "rejected_sku = ?, settled_at = ?, client_outcome = IF(? AND client_outcome = 'NONE', 'REJECTED', client_outcome) "
            + "WHERE transaction_id = ? AND attempt_number = ? AND state = 'PENDING'";
    private static final String CLIENT_OUTCOME = "UPDATE checkout_attempt SET client_outcome = ? "
            + "WHERE transaction_id = ? AND attempt_number = ? AND client_outcome = 'NONE'";
    private static final String PENDING_ERROR = "UPDATE checkout_attempt SET client_outcome = 'PENDING_ERROR', "
            + "next_retry_at = ? WHERE transaction_id = ? AND attempt_number = ? AND state = 'PENDING' "
            + "AND client_outcome = 'NONE'";
    private static final String SCHEDULE_RETRY = "UPDATE checkout_attempt SET retry_count = retry_count + 1, "
            + "next_retry_at = ?, last_error = ? WHERE transaction_id = ? AND attempt_number = ? AND state = 'PENDING'";
    private static final String COLUMNS = "SELECT transaction_id, attempt_number, state, rejection_reason, rejected_sku, "
            + "`lines`, item_count, total_amount, client_outcome, created_at, settled_at, retry_count, next_retry_at, "
            + "last_error FROM checkout_attempt";
    private static final String FIND = COLUMNS + " WHERE transaction_id = ? AND attempt_number = ?";
    /** The index is forced so the bounded recovery scan never degrades into a full scan of every attempt. */
    private static final String DUE = COLUMNS + " FORCE INDEX (idx_checkout_attempt_due) "
            + "WHERE state = 'PENDING' AND next_retry_at <= ?";
    private static final String AFTER = " AND next_retry_at >= ? AND (next_retry_at > ? OR (next_retry_at = ? "
            + "AND (transaction_id > ? OR (transaction_id = ? AND attempt_number > ?))))";
    private static final String ORDER = " ORDER BY next_retry_at, transaction_id, attempt_number LIMIT ?";

    private final ConnectionPool pool;

    public JdbcCheckoutAttemptStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public WriteResult begin(Begin begin) {
        Objects.requireNonNull(begin, "begin");
        AttemptKey key = begin.key();
        return JdbcUnitOfWork.inTransaction(pool, "transaction.store.begin", connection -> {
            try (PreparedStatement guard = connection.prepareStatement(BEGIN_GUARD)) {
                guard.setInt(1, key.attemptNumber());
                guard.setInt(2, key.attemptNumber());
                guard.setString(3, key.transactionId());
                guard.setInt(4, begin.itemCount());
                guard.setInt(5, key.attemptNumber() - 1);
                if (guard.executeUpdate() == 0) throw JdbcUnitOfWork.rollbackAndReturn(WriteResult.GUARD_MISS);
            }
            try (PreparedStatement insert = connection.prepareStatement(BEGIN_INSERT)) {
                insert.setString(1, key.transactionId());
                insert.setInt(2, key.attemptNumber());
                insert.setString(3, begin.lines());
                insert.setInt(4, begin.itemCount());
                insert.setBigDecimal(5, begin.totalAmount());
                insert.setTimestamp(6, Timestamp.from(begin.createdAt()));
                insert.setTimestamp(7, Timestamp.from(begin.nextRetryAt()));
                insert.executeUpdate();
            }
            return WriteResult.OK;
        });
    }

    @Override
    public WriteResult complete(AttemptKey key, Instant completedAt, boolean answering) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(completedAt, "completedAt");
        return JdbcUnitOfWork.inTransaction(pool, "transaction.store.complete", connection -> {
            try (PreparedStatement guard = connection.prepareStatement(COMPLETE_GUARD)) {
                guard.setTimestamp(1, Timestamp.from(completedAt));
                guard.setString(2, key.transactionId());
                guard.setInt(3, key.attemptNumber());
                if (guard.executeUpdate() == 0) throw JdbcUnitOfWork.rollbackAndReturn(WriteResult.GUARD_MISS);
            }
            try (PreparedStatement attempt = connection.prepareStatement(COMPLETE_ATTEMPT)) {
                attempt.setTimestamp(1, Timestamp.from(completedAt));
                attempt.setBoolean(2, answering);
                attempt.setString(3, key.transactionId());
                attempt.setInt(4, key.attemptNumber());
                requireSettled(attempt.executeUpdate(), key);
            }
            return WriteResult.OK;
        });
    }

    @Override
    public WriteResult reopen(AttemptKey key, RejectionReason reason, String rejectedSku, Instant settledAt,
                              boolean answering) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(settledAt, "settledAt");
        return JdbcUnitOfWork.inTransaction(pool, "transaction.store.reopen", connection -> {
            try (PreparedStatement guard = connection.prepareStatement(REOPEN_GUARD)) {
                guard.setString(1, key.transactionId());
                guard.setInt(2, key.attemptNumber());
                if (guard.executeUpdate() == 0) throw JdbcUnitOfWork.rollbackAndReturn(WriteResult.GUARD_MISS);
            }
            try (PreparedStatement attempt = connection.prepareStatement(REOPEN_ATTEMPT)) {
                attempt.setString(1, reason.name());
                attempt.setString(2, rejectedSku);
                attempt.setTimestamp(3, Timestamp.from(settledAt));
                attempt.setBoolean(4, answering);
                attempt.setString(5, key.transactionId());
                attempt.setInt(6, key.attemptNumber());
                requireSettled(attempt.executeUpdate(), key);
            }
            return WriteResult.OK;
        });
    }

    /** The active attempt of a transaction is always PENDING; anything else is a broken invariant. */
    private static void requireSettled(int updated, AttemptKey key) throws SQLException {
        if (updated != 1) throw new SQLException("Attempt " + key + " is active but not pending");
    }

    @Override
    public boolean recordClientOutcome(AttemptKey key, ClientOutcome outcome) {
        Objects.requireNonNull(key, "key");
        if (outcome == null || outcome == ClientOutcome.NONE) throw new IllegalArgumentException("outcome must be recorded");
        return JdbcUnitOfWork.inTransaction(pool, "transaction.store.clientOutcome", connection -> {
            try (PreparedStatement update = connection.prepareStatement(CLIENT_OUTCOME)) {
                update.setString(1, outcome.name());
                update.setString(2, key.transactionId());
                update.setInt(3, key.attemptNumber());
                return update.executeUpdate() == 1;
            }
        });
    }

    @Override
    public boolean markPendingError(AttemptKey key, Instant now) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(now, "now");
        return JdbcUnitOfWork.inTransaction(pool, "transaction.store.pendingError", connection -> {
            try (PreparedStatement update = connection.prepareStatement(PENDING_ERROR)) {
                update.setTimestamp(1, Timestamp.from(now));
                update.setString(2, key.transactionId());
                update.setInt(3, key.attemptNumber());
                return update.executeUpdate() == 1;
            }
        });
    }

    @Override
    public List<AttemptRecord> findPending(Instant dueBefore, PageCursor after, int limit) {
        Objects.requireNonNull(dueBefore, "dueBefore");
        if (limit < 1) throw new IllegalArgumentException("limit must be positive");
        return JdbcUnitOfWork.read(pool, "transaction.store.findPending", connection -> {
            try (PreparedStatement select = connection.prepareStatement(pendingQuery(after != null))) {
                int index = 1;
                select.setTimestamp(index++, Timestamp.from(dueBefore));
                if (after != null) {
                    Timestamp at = Timestamp.from(after.nextRetryAt());
                    select.setTimestamp(index++, at);
                    select.setTimestamp(index++, at);
                    select.setTimestamp(index++, at);
                    select.setString(index++, after.key().transactionId());
                    select.setString(index++, after.key().transactionId());
                    select.setInt(index++, after.key().attemptNumber());
                }
                select.setInt(index, limit);
                List<AttemptRecord> page = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) page.add(record(rows));
                }
                return page;
            }
        });
    }

    /** The recovery scan's statement, first page or a later one; package-private so its plan can be checked. */
    static String pendingQuery(boolean afterCursor) {
        return DUE + (afterCursor ? AFTER : "") + ORDER;
    }

    @Override
    public Optional<AttemptRecord> findAttempt(AttemptKey key) {
        Objects.requireNonNull(key, "key");
        return JdbcUnitOfWork.read(pool, "transaction.store.findAttempt", connection -> find(connection, key));
    }

    private static Optional<AttemptRecord> find(Connection connection, AttemptKey key) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(FIND)) {
            select.setString(1, key.transactionId());
            select.setInt(2, key.attemptNumber());
            try (ResultSet row = select.executeQuery()) {
                return row.next() ? Optional.of(record(row)) : Optional.empty();
            }
        }
    }

    @Override
    public void scheduleRetry(AttemptKey key, Instant nextRetryAt, String lastError) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(nextRetryAt, "nextRetryAt");
        String error = lastError == null || lastError.length() <= LAST_ERROR_LENGTH ? lastError
                : lastError.substring(0, LAST_ERROR_LENGTH);
        JdbcUnitOfWork.inTransaction(pool, "transaction.store.scheduleRetry", connection -> {
            try (PreparedStatement update = connection.prepareStatement(SCHEDULE_RETRY)) {
                update.setTimestamp(1, Timestamp.from(nextRetryAt));
                update.setString(2, error);
                update.setString(3, key.transactionId());
                update.setInt(4, key.attemptNumber());
                update.executeUpdate();
            }
            return null;
        });
    }

    private static AttemptRecord record(ResultSet row) throws SQLException {
        String reason = row.getString("rejection_reason");
        return new AttemptRecord(new AttemptKey(row.getString("transaction_id"), row.getInt("attempt_number")),
                State.valueOf(row.getString("state")), reason == null ? null : RejectionReason.valueOf(reason),
                row.getString("rejected_sku"), row.getString("lines"), row.getInt("item_count"),
                row.getBigDecimal("total_amount"), ClientOutcome.valueOf(row.getString("client_outcome")),
                instant(row.getTimestamp("created_at")), instant(row.getTimestamp("settled_at")),
                row.getInt("retry_count"), instant(row.getTimestamp("next_retry_at")), row.getString("last_error"));
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
