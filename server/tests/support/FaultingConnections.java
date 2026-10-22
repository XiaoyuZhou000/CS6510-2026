package support;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only {@code ConnectionPool} decorator that scripts {@code commit()} faults for one thread or
 * one connection (R19, R22):
 * <ul>
 *   <li>{@link Kind#FAIL_BEFORE_COMMIT} throws without delegating, so nothing commits;</li>
 *   <li>{@link Kind#FAIL_AFTER_COMMIT} delegates and then throws, so the work is durable but the
 *       caller sees a failure;</li>
 *   <li>{@link Kind#DELAY} sleeps before delegating.</li>
 * </ul>
 * A fault fires for the given number of commits (once by default) and is then disarmed.
 */
public final class FaultingConnections {
    public enum Kind { FAIL_BEFORE_COMMIT, FAIL_AFTER_COMMIT, DELAY }

    /** One scripted fault; {@code times} commits are affected. */
    public record CommitFault(Kind kind, long delayMillis, int times) {
        public static CommitFault failBeforeCommit() { return new CommitFault(Kind.FAIL_BEFORE_COMMIT, 0, 1); }
        public static CommitFault failAfterCommit() { return new CommitFault(Kind.FAIL_AFTER_COMMIT, 0, 1); }
        public static CommitFault delay(long millis) { return new CommitFault(Kind.DELAY, millis, 1); }
        public CommitFault times(int count) {
            if (count < 1) throw new IllegalArgumentException("times must be positive");
            return new CommitFault(kind, delayMillis, count);
        }
    }

    private final Map<Thread, Armed> byThread = new ConcurrentHashMap<>();
    private final Map<Connection, Armed> byConnection = new ConcurrentHashMap<>();
    private final AtomicLong commitsStarted = new AtomicLong();
    private final AtomicLong commitsReturned = new AtomicLong();
    private final AtomicLong faultsFired = new AtomicLong();

    private record Armed(CommitFault fault, AtomicInteger remaining) {
        Armed(CommitFault fault) { this(fault, new AtomicInteger(fault.times())); }
    }

    /** Arms a fault for the next commits made on {@code thread}, through any pooled connection. */
    public void armForThread(Thread thread, CommitFault fault) {
        byThread.put(thread, new Armed(fault));
    }

    public void armForCurrentThread(CommitFault fault) {
        armForThread(Thread.currentThread(), fault);
    }

    /**
     * Arms a fault for the next commits made through one pooled connection, as the pool hands it
     * out; this decorator must then be the outermost one.
     */
    public void armForConnection(Connection pooled, CommitFault fault) {
        byConnection.put(pooled, new Armed(fault));
    }

    public void disarmAll() {
        byThread.clear();
        byConnection.clear();
    }

    /** Commits that reached this decorator, including faulted ones. */
    public long commitsStarted() { return commitsStarted.get(); }
    /** Commits that returned normally to the caller. */
    public long commitsReturned() { return commitsReturned.get(); }
    public long faultsFired() { return faultsFired.get(); }

    /** The decorator to pass to {@code ConnectionPool}; it composes with other decorators. */
    public Connection wrap(Connection connection) {
        Connection[] self = new Connection[1];
        self[0] = CountingConnections.proxy(Connection.class, connection, (target, method, args) -> {
            if (!method.getName().equals("commit")) return method.invoke(target, args);
            commitsStarted.incrementAndGet();
            CommitFault fault = take(self[0]);
            if (fault != null) faultsFired.incrementAndGet();
            if (fault != null && fault.kind() == Kind.FAIL_BEFORE_COMMIT) {
                throw new SQLException("Injected failure before commit", "08S01");
            }
            if (fault != null && fault.kind() == Kind.DELAY) Thread.sleep(fault.delayMillis());
            Object result = method.invoke(target, args);
            if (fault != null && fault.kind() == Kind.FAIL_AFTER_COMMIT) {
                throw new SQLException("Injected failure after a successful commit", "08S01");
            }
            commitsReturned.incrementAndGet();
            return result;
        });
        return self[0];
    }

    private CommitFault take(Connection pooled) {
        CommitFault fault = take(byConnection, pooled);
        return fault != null ? fault : take(byThread, Thread.currentThread());
    }

    private static <K> CommitFault take(Map<K, Armed> armed, K key) {
        Armed entry = armed.get(key);
        if (entry == null) return null;
        int left = entry.remaining().decrementAndGet();
        if (left <= 0) armed.remove(key, entry);
        return left >= 0 ? entry.fault() : null;
    }
}
