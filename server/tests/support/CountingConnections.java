package support;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only {@code ConnectionPool} decorator that counts round trips (R22). Every execution is
 * classified by SQL verb, and {@code commit}, {@code rollback}, {@code setAutoCommit}, and
 * {@code setTransactionIsolation} calls are counted. Counts are kept in total and in the scope that
 * is open on the calling thread, so a test can measure exactly one operation while other threads run.
 *
 * <pre>{@code
 * CountingConnections counting = new CountingConnections();
 * ConnectionPool pool = schema.newPool("t", 2, 4, counting::wrap);
 * try (CountingConnections.Scope scope = counting.scope()) {
 *     store.appendScan(...);
 *     assertEquals(2, scope.counts().statements());
 * }
 * }</pre>
 */
public final class CountingConnections {
    private static final Set<String> EXECUTE = Set.of("execute", "executeQuery", "executeUpdate",
            "executeLargeUpdate", "executeBatch", "executeLargeBatch");

    private final Counts total = new Counts();
    private final ThreadLocal<Counts> current = new ThreadLocal<>();

    public enum Verb { SELECT, INSERT, UPDATE, DELETE, OTHER }

    /** Counters for one scope or for the total. */
    public static final class Counts {
        private final AtomicLong[] verbs = new AtomicLong[Verb.values().length];
        private final AtomicLong commits = new AtomicLong();
        private final AtomicLong rollbacks = new AtomicLong();
        private final AtomicLong autoCommitCalls = new AtomicLong();
        private final AtomicLong isolationCalls = new AtomicLong();

        Counts() {
            for (int i = 0; i < verbs.length; i++) verbs[i] = new AtomicLong();
        }

        public long count(Verb verb) { return verbs[verb.ordinal()].get(); }
        public long selects() { return count(Verb.SELECT); }
        public long inserts() { return count(Verb.INSERT); }
        public long updates() { return count(Verb.UPDATE); }
        public long deletes() { return count(Verb.DELETE); }
        public long others() { return count(Verb.OTHER); }
        /** Every statement execution, whatever its verb. */
        public long statements() {
            long sum = 0;
            for (AtomicLong verb : verbs) sum += verb.get();
            return sum;
        }
        public long commits() { return commits.get(); }
        public long rollbacks() { return rollbacks.get(); }
        public long setAutoCommitCalls() { return autoCommitCalls.get(); }
        public long setTransactionIsolationCalls() { return isolationCalls.get(); }

        @Override
        public String toString() {
            return "Counts[select=" + selects() + ", insert=" + inserts() + ", update=" + updates()
                    + ", delete=" + deletes() + ", other=" + others() + ", commit=" + commits()
                    + ", rollback=" + rollbacks() + ", setAutoCommit=" + setAutoCommitCalls()
                    + ", setTransactionIsolation=" + setTransactionIsolationCalls() + "]";
        }
    }

    /** Counting on the calling thread; closing it ends the scope. Scopes do not nest. */
    public final class Scope implements AutoCloseable {
        private final Counts counts = new Counts();
        private final Thread owner = Thread.currentThread();

        private Scope() {
            if (current.get() != null) throw new IllegalStateException("A counting scope is already open on this thread");
            current.set(counts);
        }

        public Counts counts() { return counts; }

        @Override
        public void close() {
            if (Thread.currentThread() != owner) throw new IllegalStateException("Close the scope on its own thread");
            current.remove();
        }
    }

    public Scope scope() { return new Scope(); }

    /** Counts from every thread since this decorator was created. */
    public Counts total() { return total; }

    /** The decorator to pass to {@code ConnectionPool}. */
    public Connection wrap(Connection connection) {
        return proxy(Connection.class, connection, (target, method, args) -> {
            switch (method.getName()) {
                case "commit" -> record(c -> c.commits.incrementAndGet());
                case "rollback" -> record(c -> c.rollbacks.incrementAndGet());
                case "setAutoCommit" -> record(c -> c.autoCommitCalls.incrementAndGet());
                case "setTransactionIsolation" -> record(c -> c.isolationCalls.incrementAndGet());
                default -> { }
            }
            Object result = method.invoke(target, args);
            if (result instanceof CallableStatement callable) {
                return statement(CallableStatement.class, callable, (String) args[0]);
            }
            if (result instanceof PreparedStatement prepared && method.getName().startsWith("prepare")) {
                return statement(PreparedStatement.class, prepared, (String) args[0]);
            }
            if (result instanceof Statement plain && method.getName().equals("createStatement")) {
                return statement(Statement.class, plain, null);
            }
            return result;
        });
    }

    private <S extends Statement> S statement(Class<S> type, S statement, String preparedSql) {
        String[] batchSql = new String[1];
        return proxy(type, statement, (target, method, args) -> {
            String name = method.getName();
            if (name.equals("addBatch") && args != null && args.length == 1 && batchSql[0] == null) {
                batchSql[0] = (String) args[0];
            }
            if (EXECUTE.contains(name)) {
                String sql = args != null && args.length > 0 && args[0] instanceof String text ? text
                        : preparedSql != null ? preparedSql : batchSql[0];
                Verb verb = verb(sql);
                record(c -> c.verbs[verb.ordinal()].incrementAndGet());
                if (name.contains("Batch")) batchSql[0] = null;
            }
            return method.invoke(target, args);
        });
    }

    /** The statement's verb: its first keyword, with WITH counted as SELECT and REPLACE as INSERT. */
    public static Verb verb(String sql) {
        if (sql == null) return Verb.OTHER;
        String text = sql.stripLeading();
        while (text.startsWith("(")) text = text.substring(1).stripLeading();
        int end = 0;
        while (end < text.length() && Character.isLetter(text.charAt(end))) end++;
        return switch (text.substring(0, end).toUpperCase(Locale.ROOT)) {
            case "SELECT", "WITH" -> Verb.SELECT;
            case "INSERT", "REPLACE" -> Verb.INSERT;
            case "UPDATE" -> Verb.UPDATE;
            case "DELETE" -> Verb.DELETE;
            default -> Verb.OTHER;
        };
    }

    private void record(java.util.function.Consumer<Counts> increment) {
        increment.accept(total);
        Counts scoped = current.get();
        if (scoped != null) increment.accept(scoped);
    }

    @FunctionalInterface
    interface Handler {
        Object handle(Object target, Method method, Object[] args) throws Throwable;
    }

    /** A JDK proxy with identity equality whose delegate exceptions are unwrapped. */
    static <T> T proxy(Class<T> type, T target, Handler handler) {
        InvocationHandler invocation = (self, method, args) -> {
            switch (method.getName()) {
                case "equals" -> { return args[0] == self; }
                case "hashCode" -> { return System.identityHashCode(self); }
                case "toString" -> { return "Counted[" + target + "]"; }
                case "unwrap" -> {
                    if (((Class<?>) args[0]).isInstance(self)) return self;
                    return method.invoke(target, args);
                }
                default -> { }
            }
            try {
                return handler.handle(target, method, args);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        };
        return type.cast(Proxy.newProxyInstance(CountingConnections.class.getClassLoader(), new Class<?>[]{type}, invocation));
    }
}
