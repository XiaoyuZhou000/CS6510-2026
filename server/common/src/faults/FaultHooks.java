package faults;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Test-only fault injection points named in the contracts (R15).
 *
 * <p>The JVM property {@code selfcheckout.faults} lists comma-separated {@code name:action} entries,
 * where an action is {@code halt} ({@code Runtime.halt(137)}), {@code delay:<ms>}, or {@code throw}.
 * Without the property every hook is a no-op. The list is parsed again only when the property's
 * value changes.</p>
 *
 * <p>Channel faults (007 research N13) are flags instead of actions: {@code notification.channel.reject:<id>}
 * and {@code notification.channel.stall:<id>} name one subscriber's channel, may be listed for several
 * subscribers, and are read with {@link #isSet}, by Notification's channel wrapper only.</p>
 */
public final class FaultHooks {
    public static final String PROPERTY = "selfcheckout.faults";
    public static final String CHANNEL_REJECT = "notification.channel.reject";
    public static final String CHANNEL_STALL = "notification.channel.stall";
    private static final Pattern CHANNEL_FLAG =
            Pattern.compile("^notification\\.channel\\.(reject|stall):[A-Za-z0-9_-]{1,40}$");
    private static volatile Parsed parsed = new Parsed(null, Map.of(), Set.of());

    private FaultHooks() { }

    public static void hit(String name) {
        Action action = current().actions().get(name);
        if (action != null) action.run(name);
    }

    /** Whether the flag entry {@code flag}, for example {@code notification.channel.reject:sub-1}, is set. */
    public static boolean isSet(String flag) {
        return current().flags().contains(flag);
    }

    private static Parsed current() {
        String raw = System.getProperty(PROPERTY);
        if (raw == null) return Parsed.EMPTY;
        Parsed current = parsed;
        if (!raw.equals(current.raw())) {
            current = new Parsed(raw, parse(raw), flags(raw));
            parsed = current;
        }
        return current;
    }

    /** The channel-fault flags of a property value, in order; malformed flags are rejected. */
    public static Set<String> flags(String raw) {
        if (raw == null || raw.isBlank()) return Set.of();
        Set<String> flags = new LinkedHashSet<>();
        for (String entry : raw.split(",")) {
            String trimmed = entry.trim();
            if (!trimmed.startsWith("notification.channel.")) continue;
            if (!CHANNEL_FLAG.matcher(trimmed).matches()) {
                throw new IllegalArgumentException("Channel fault must be notification.channel.(reject|stall):<subscriberId>: "
                        + trimmed);
            }
            flags.add(trimmed);
        }
        return Set.copyOf(flags);
    }

    /**
     * Parses a property value's actions; malformed entries are rejected so a test cannot silently miss a hook.
     * Channel-fault flags are skipped here and read by {@link #flags}.
     */
    public static Map<String, Action> parse(String raw) {
        Map<String, Action> actions = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return Map.of();
        for (String entry : raw.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("notification.channel.")) continue;
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

    private record Parsed(String raw, Map<String, Action> actions, Set<String> flags) {
        static final Parsed EMPTY = new Parsed(null, Map.of(), Set.of());
    }
}
