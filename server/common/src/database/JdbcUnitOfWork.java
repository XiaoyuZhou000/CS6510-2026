package database;

import metrics.PerfMetrics;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Runs one local transaction on a managed {@link ConnectionPool} connection (R20). The pool already
 * keeps sessions in manual-commit mode at READ COMMITTED, so no unit toggles autocommit or isolation.
 *
 * <ul>
 *   <li>Any failure before commit rolls back, with an explicit {@code ROLLBACK} statement (see
 *       {@link #abort}).</li>
 *   <li>A deadlock (MySQL 1213 or SQLState 40001) before commit retries the whole unit, at most
 *       {@value #MAX_ATTEMPTS} attempts with 10 ms × attempt backoff; exhaustion raises a
 *       {@link StoreFailure#contended()} failure. {@link #inContendedTransaction} also retries
 *       lock-wait timeouts (MySQL 1205) this way.</li>
 *   <li>A failing {@code commit()} is never retried: it raises {@link StoreFailure} with
 *       {@code commitOutcomeUnknown}, because the work may already be durable.</li>
 * </ul>
 */
public final class JdbcUnitOfWork {
    public static final int MAX_ATTEMPTS = 3;
    private static final int LOCK_WAIT_TIMEOUT = 1205;

    private JdbcUnitOfWork() { }

    @FunctionalInterface
    public interface Work<T> {
        T run(Connection connection) throws SQLException;
    }

    /** Runs {@code work} and commits; the unit is timed into {@code metricName}. */
    public static <T> T inTransaction(ConnectionPool pool, String metricName, Work<T> work) {
        return run(pool, metricName, work, false);
    }

    /**
     * Like {@link #inTransaction}, for units that lock rows other requests contend for: a lock-wait
     * timeout retries like a deadlock, so contention is never reported as anything but contention (R8).
     */
    public static <T> T inContendedTransaction(ConnectionPool pool, String metricName, Work<T> work) {
        return run(pool, metricName, work, true);
    }

    private static <T> T run(ConnectionPool pool, String metricName, Work<T> work, boolean lockWaitRetries) {
        long started = System.nanoTime();
        try {
            for (int attempt = 1; ; attempt++) {
                Connection connection = pool.borrow();
                boolean deadlocked = false;
                try {
                    T result;
                    try {
                        result = work.run(connection);
                    } catch (RollbackAndReturn rollback) {
                        connection.rollback();
                        @SuppressWarnings("unchecked") T value = (T) rollback.value;
                        return value;
                    } catch (SQLException failure) {
                        rollbackQuietly(connection, failure);
                        boolean retryable = isDeadlock(failure) || (lockWaitRetries && isLockWaitTimeout(failure));
                        if (!retryable) throw new StoreFailure("Database unit of work failed", failure);
                        if (attempt >= MAX_ATTEMPTS) {
                            throw StoreFailure.contended("Database unit of work met contention " + attempt + " times",
                                    failure);
                        }
                        deadlocked = true;
                        result = null;
                    } catch (RuntimeException | Error failure) {
                        rollbackQuietly(connection, failure);
                        throw failure;
                    }
                    if (!deadlocked) {
                        long committing = System.nanoTime();
                        try {
                            connection.commit();
                        } catch (SQLException failure) {
                            PerfMetrics.increment(metricName + ".commitUnknown");
                            throw new StoreFailure("Database commit outcome is unknown", failure, true);
                        } finally {
                            PerfMetrics.record(metricName + ".commit", System.nanoTime() - committing);
                        }
                        return result;
                    }
                } catch (SQLException rollbackFailure) {
                    throw new StoreFailure("Database rollback failed", rollbackFailure);
                } finally {
                    pool.release(connection);
                }
                PerfMetrics.increment(metricName + ".deadlockRetries");
                backoff(attempt);
            }
        } finally {
            PerfMetrics.record(metricName, System.nanoTime() - started);
        }
    }

    /** Runs read-only {@code work} and ends the unit with a rollback. */
    public static <T> T read(ConnectionPool pool, String metricName, Work<T> work) {
        long started = System.nanoTime();
        Connection connection = pool.borrow();
        try {
            T result = work.run(connection);
            connection.rollback();
            return result;
        } catch (SQLException failure) {
            rollbackQuietly(connection, failure);
            throw new StoreFailure("Database read failed", failure);
        } catch (RuntimeException | Error failure) {
            rollbackQuietly(connection, failure);
            throw failure;
        } finally {
            pool.release(connection);
            PerfMetrics.record(metricName, System.nanoTime() - started);
        }
    }

    /**
     * Ends the current {@link #inTransaction} unit with a rollback instead of a commit and makes it
     * return {@code value}, for example a guarded UPDATE that matched no row.
     */
    public static RollbackAndReturn rollbackAndReturn(Object value) {
        return new RollbackAndReturn(value);
    }

    public static boolean isDeadlock(SQLException failure) {
        for (SQLException e = failure; e != null; e = e.getNextException()) {
            if (e.getErrorCode() == 1213 || "40001".equals(e.getSQLState())) return true;
            if (e.getCause() instanceof SQLException cause && cause != e && isDeadlock(cause)) return true;
        }
        return false;
    }

    public static boolean isLockWaitTimeout(SQLException failure) {
        for (SQLException e = failure; e != null; e = e.getNextException()) {
            if (e.getErrorCode() == LOCK_WAIT_TIMEOUT) return true;
            if (e.getCause() instanceof SQLException cause && cause != e && isLockWaitTimeout(cause)) return true;
        }
        return false;
    }

    /**
     * Ends whatever transaction the session has open after a failed statement.
     *
     * <p>With {@code useLocalTransactionState=true} (R20), {@code Connection.rollback()} is skipped when
     * the driver believes no transaction is open, and the driver learns that only from OK packets. A
     * failed first statement returns an error packet, so the server keeps a transaction, with its row
     * and metadata locks, that the driver does not know about; that session would carry the locks back
     * into the pool. A {@code ROLLBACK} statement is always sent.</p>
     */
    public static void abort(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("ROLLBACK");
        }
    }

    private static void rollbackQuietly(Connection connection, Throwable failure) {
        try {
            abort(connection);
        } catch (SQLException rollback) {
            failure.addSuppressed(rollback);
        }
    }

    private static void backoff(int attempt) {
        try {
            Thread.sleep(10L * attempt);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new StoreFailure("Interrupted while retrying a deadlocked unit of work", interrupted);
        }
    }

    /** Control-flow signal for {@link #rollbackAndReturn}; carries no stack trace. */
    public static final class RollbackAndReturn extends RuntimeException {
        private final transient Object value;

        private RollbackAndReturn(Object value) {
            super("rollback and return", null, false, false);
            this.value = value;
        }
    }
}
