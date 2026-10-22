package rpc;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test-only fault injection points named in the contracts (R15).
 *
 * <p>The JVM property {@code selfcheckout.faults} lists comma-separated {@code name:action} entries,
 * where an action is {@code halt} ({@code Runtime.halt(137)}), {@code delay:<ms>}, or {@code throw}.
 * Without the property every hook is a no-op. The list is parsed again only when the property's
 * value changes.</p>
 */
public final class FaultHooks {
    public static final String PROPERTY = "selfcheckout.faults";
    private static volatile Parsed parsed = new Parsed(null, Map.of());

    private FaultHooks() { }

    public static void hit(String name) {
        String raw = System.getProperty(PROPERTY);
        if (raw == null) return;
        Parsed current = parsed;
        if (!raw.equals(current.raw())) {
            current = new Parsed(raw, parse(raw));
            parsed = current;
        }
        Action action = current.actions().get(name);
        if (action != null) action.run(name);
    }

    /** Parses a property value; malformed entries are rejected so a test cannot silently miss a hook. */
    public static Map<String, Action> parse(String raw) {
        Map<String, Action> actions = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return Map.of();
        for (String entry : raw.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            int separator = trimmed.indexOf(':');
            if (separator <= 0 || separator == trimmed.length() - 1) {
                throw new IllegalArgumentException("Fault entry must be name:action: " + trimmed);
            }
            String name = trimmed.substring(0, separator);
            String action = trimmed.substring(separator + 1);
            if (actions.put(name, Action.parse(action)) != null) {
                throw new IllegalArgumentException("Fault hook listed twice: " + name);
            }
        }
        return Map.copyOf(actions);
    }

    public enum Kind { HALT, DELAY, THROW }

    public record Action(Kind kind, long delayMillis) {
        static Action parse(String action) {
            if (action.equals("halt")) return new Action(Kind.HALT, 0);
            if (action.equals("throw")) return new Action(Kind.THROW, 0);
            if (action.startsWith("delay:")) {
                long millis;
                try {
                    millis = Long.parseLong(action.substring("delay:".length()));
                } catch (NumberFormatException malformed) {
                    throw new IllegalArgumentException("Fault delay must be a number of milliseconds: " + action);
                }
                if (millis < 0) throw new IllegalArgumentException("Fault delay must not be negative: " + action);
                return new Action(Kind.DELAY, millis);
            }
            throw new IllegalArgumentException("Unknown fault action: " + action);
        }

        void run(String hook) {
            switch (kind) {
                case HALT -> {
                    System.out.flush();
                    System.err.flush();
                    Runtime.getRuntime().halt(137);
                }
                case DELAY -> {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                case THROW -> throw new InjectedFault(hook);
            }
        }
    }

    /** The failure raised by a {@code throw} hook. */
    public static final class InjectedFault extends RuntimeException {
        public InjectedFault(String hook) {
            super("Injected fault at " + hook);
        }
    }

    private record Parsed(String raw, Map<String, Action> actions) { }
}
