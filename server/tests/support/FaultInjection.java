package support;

import rpc.FaultHooks;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Builds {@code selfcheckout.faults} specifications for the hooks named in the contracts (R15).
 *
 * <pre>{@code
 * String spec = FaultInjection.spec().halt("relay.afterAck").delay("inventory.apply.beforeCommit", 3000).build();
 * ServiceProcesses.service("transaction").faults(spec)...;      // in a child JVM
 * try (var armed = FaultInjection.spec().fail("x").armInThisJvm()) { ... }   // in-process
 * }</pre>
 */
public final class FaultInjection {
    private final Map<String, String> actions = new LinkedHashMap<>();

    private FaultInjection() { }

    public static FaultInjection spec() { return new FaultInjection(); }

    public FaultInjection halt(String hook) { return put(hook, "halt"); }

    public FaultInjection delay(String hook, long millis) {
        if (millis < 0) throw new IllegalArgumentException("delay must not be negative");
        return put(hook, "delay:" + millis);
    }

    /** The hook throws {@link FaultHooks.InjectedFault}. */
    public FaultInjection fail(String hook) { return put(hook, "throw"); }

    private FaultInjection put(String hook, String action) {
        if (hook == null || !hook.matches("[A-Za-z0-9_.]+")) throw new IllegalArgumentException("Bad hook name: " + hook);
        if (actions.putIfAbsent(hook, action) != null) throw new IllegalArgumentException("Hook listed twice: " + hook);
        return this;
    }

    public String build() {
        StringJoiner spec = new StringJoiner(",");
        actions.forEach((hook, action) -> spec.add(hook + ":" + action));
        String built = spec.toString();
        FaultHooks.parse(built);
        return built;
    }

    /** Sets the property in this JVM until the returned handle is closed, restoring the previous value. */
    public Armed armInThisJvm() {
        String previous = System.getProperty(FaultHooks.PROPERTY);
        System.setProperty(FaultHooks.PROPERTY, build());
        return () -> {
            if (previous == null) System.clearProperty(FaultHooks.PROPERTY);
            else System.setProperty(FaultHooks.PROPERTY, previous);
        };
    }

    @FunctionalInterface
    public interface Armed extends AutoCloseable {
        @Override
        void close();
    }
}
