package analytics;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/** Injectable JSON-lines logger for analytics operational events. */
public final class AnalyticsLog {
    private static final Pattern CREDENTIAL = Pattern.compile(
            "(?i)(password|passwd|pwd|user(?:name)?|api[_-]?key|access[_-]?token|"
                    + "refresh[_-]?token|token|secret)\\s*[=:]\\s*[^\\s,;]+",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern JDBC_URL = Pattern.compile("(?i)jdbc:[^\\s]+", Pattern.CASE_INSENSITIVE);
    private static final Pattern SQL = Pattern.compile(
            "(?is)\\b(select|insert|update|delete|merge|alter|create|drop|truncate)\\b.+");
    private static final Pattern BASKET_DATA = Pattern.compile(
            "(?is)\\b(basket|cart)(Id|Items?|Contents?)?\\b.+");
    private static final Pattern STACK_TRACE = Pattern.compile(
            "(?is)\\s+(?:at\\s+[A-Za-z_$][\\w$]*(?:\\.[A-Za-z_$][\\w$]*)+\\([^)]*\\)|stack\\s+trace).*");

    private final Clock clock;
    private final Consumer<String> sink;

    public AnalyticsLog(Consumer<String> sink) {
        this(Clock.systemUTC(), sink);
    }

    public AnalyticsLog(Clock clock, Consumer<String> sink) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.sink = Objects.requireNonNull(sink, "sink must not be null");
    }

    /** Production logger: one JSON object on one stderr line. */
    public static AnalyticsLog stderr() {
        return new AnalyticsLog(System.err::println);
    }

    public void write(Event event) {
        sink.accept(toJson(Objects.requireNonNull(event, "event must not be null")));
    }

    public void backlog(Stage stage, String queue, int queueDepth) {
        write(Event.backlog(clock.instant(), stage, queue, queueDepth));
    }

    public void persistenceRetry(int retryCount, long windowStart, long windowEnd, Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        write(Event.persistenceRetry(clock.instant(), retryCount, windowStart, windowEnd,
                failure.getClass().getSimpleName()));
    }

    public void ingestionRejected(String state, String reason) {
        write(Event.ingestionRejected(clock.instant(), state, reason));
    }

    public void drainStarted(int ingressQueueDepth, int windowQueueDepth, int rankingQueueDepth) {
        write(Event.drainStarted(clock.instant(), ingressQueueDepth, windowQueueDepth,
                rankingQueueDepth));
    }

    public void shutdownCompleted() {
        write(Event.shutdown(clock.instant(), EventType.SHUTDOWN_COMPLETED,
                ShutdownOutcome.DRAINED));
    }

    public void shutdownForced(ShutdownOutcome outcome) {
        write(Event.shutdown(clock.instant(), EventType.SHUTDOWN_FORCED, outcome));
    }

    public void workerFailed(Stage stage, Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        write(Event.workerFailed(clock.instant(), stage, failure.getClass().getSimpleName(),
                failure.getMessage() == null ? "worker terminated unexpectedly" : failure.getMessage()));
    }

    public enum EventType {
        BACKLOG,
        PERSISTENCE_RETRY,
        INGESTION_REJECTED,
        DRAIN_STARTED,
        SHUTDOWN_COMPLETED,
        SHUTDOWN_FORCED,
        WORKER_FAILED
    }

    public enum Stage {
        INGRESS,
        WINDOW,
        RANKING,
        PERSISTENCE,
        PIPELINE
    }

    public enum ShutdownOutcome {
        DRAINED,
        TIMEOUT,
        INTERRUPTED
    }

    /**
     * Validated structured record. Nullable event-specific fields are omitted from JSON rather
     * than serialized as null.
     */
    public record Event(
            Instant timestamp,
            EventType eventType,
            Stage stage,
            String queue,
            Integer queueDepth,
            Integer retryCount,
            Long windowStart,
            Long windowEnd,
            ShutdownOutcome shutdownOutcome,
            String state,
            String errorType,
            String reason,
            Integer ingressQueueDepth,
            Integer windowQueueDepth,
            Integer rankingQueueDepth) {

        public Event {
            Objects.requireNonNull(timestamp, "timestamp must not be null");
            Objects.requireNonNull(eventType, "eventType must not be null");
            Objects.requireNonNull(stage, "stage must not be null");
            if (queueDepth != null && queueDepth < 0) {
                throw new IllegalArgumentException("queueDepth must not be negative");
            }
            if (retryCount != null && retryCount <= 0) {
                throw new IllegalArgumentException("retryCount must be positive");
            }
            if ((windowStart != null && windowStart <= 0) || (windowEnd != null && windowEnd <= 0)) {
                throw new IllegalArgumentException("window bounds must be positive when supplied");
            }
            if (windowStart != null && windowEnd != null && windowEnd < windowStart) {
                throw new IllegalArgumentException("windowEnd must not precede windowStart");
            }

            if (eventType == EventType.BACKLOG) {
                requiredText(queue, "queue");
                Objects.requireNonNull(queueDepth, "queueDepth is required for backlog events");
            }
            if (eventType == EventType.PERSISTENCE_RETRY) {
                Objects.requireNonNull(retryCount, "retryCount is required for persistence retry");
                Objects.requireNonNull(windowStart, "windowStart is required for persistence retry");
                Objects.requireNonNull(windowEnd, "windowEnd is required for persistence retry");
                requiredText(errorType, "errorType");
            }
            if (eventType == EventType.INGESTION_REJECTED) {
                requiredText(state, "state");
                requiredText(reason, "reason");
            }
            if (eventType == EventType.DRAIN_STARTED) {
                requireNonNegative(ingressQueueDepth, "ingressQueueDepth");
                requireNonNegative(windowQueueDepth, "windowQueueDepth");
                requireNonNegative(rankingQueueDepth, "rankingQueueDepth");
            }
            if (eventType == EventType.SHUTDOWN_COMPLETED
                    || eventType == EventType.SHUTDOWN_FORCED) {
                Objects.requireNonNull(
                        shutdownOutcome, "shutdownOutcome is required for shutdown events");
            }
            if (eventType == EventType.SHUTDOWN_COMPLETED
                    && shutdownOutcome != ShutdownOutcome.DRAINED) {
                throw new IllegalArgumentException("completed shutdown outcome must be DRAINED");
            }
            if (eventType == EventType.SHUTDOWN_FORCED
                    && shutdownOutcome == ShutdownOutcome.DRAINED) {
                throw new IllegalArgumentException("forced shutdown outcome must identify its failure");
            }
            if (eventType == EventType.WORKER_FAILED) {
                requiredText(errorType, "errorType");
                requiredText(reason, "reason");
            }

            queue = sanitize(queue);
            state = sanitize(state);
            errorType = sanitizeIdentifier(errorType);
            reason = sanitize(reason);
        }

        public static Event of(Instant timestamp, EventType eventType, Stage stage) {
            return new Event(timestamp, eventType, stage, null, null, null, null, null,
                    null, null, null, null, null, null, null);
        }

        public static Event backlog(Instant timestamp, Stage stage, String queue, int queueDepth) {
            return new Event(timestamp, EventType.BACKLOG, stage, queue, queueDepth, null,
                    null, null, null, null, null, null, null, null, null);
        }

        public static Event persistenceRetry(
                Instant timestamp, int retryCount, long windowStart, long windowEnd, String errorType) {
            return new Event(timestamp, EventType.PERSISTENCE_RETRY, Stage.PERSISTENCE,
                    null, null, retryCount, windowStart, windowEnd, null, null, errorType, null,
                    null, null, null);
        }

        public static Event ingestionRejected(Instant timestamp, String state, String reason) {
            return new Event(timestamp, EventType.INGESTION_REJECTED, Stage.INGRESS,
                    null, null, null, null, null, null, state, null, reason, null, null, null);
        }

        public static Event drainStarted(Instant timestamp, int ingressQueueDepth,
                int windowQueueDepth, int rankingQueueDepth) {
            return new Event(timestamp, EventType.DRAIN_STARTED, Stage.PIPELINE,
                    null, null, null, null, null, null, "DRAINING", null, null,
                    ingressQueueDepth, windowQueueDepth, rankingQueueDepth);
        }

        public static Event shutdown(
                Instant timestamp, EventType eventType, ShutdownOutcome shutdownOutcome) {
            return new Event(timestamp, eventType, Stage.PIPELINE, null, null, null,
                    null, null, shutdownOutcome, null, null, null, null, null, null);
        }

        public static Event workerFailed(
                Instant timestamp, Stage stage, String errorType, String reason) {
            return new Event(timestamp, EventType.WORKER_FAILED, stage, null, null, null,
                    null, null, null, null, errorType, reason, null, null, null);
        }

        private static void requireNonNegative(Integer value, String field) {
            Objects.requireNonNull(value, field + " is required for drain-started events");
            if (value < 0) throw new IllegalArgumentException(field + " must not be negative");
        }
    }

    private static String toJson(Event event) {
        StringBuilder json = new StringBuilder(192).append('{');
        append(json, "timestamp", event.timestamp().toString());
        append(json, "eventType", event.eventType().name());
        append(json, "stage", event.stage().name());
        append(json, "queue", event.queue());
        append(json, "queueDepth", event.queueDepth());
        append(json, "retryCount", event.retryCount());
        append(json, "windowStart", event.windowStart());
        append(json, "windowEnd", event.windowEnd());
        append(json, "shutdownOutcome",
                event.shutdownOutcome() == null ? null : event.shutdownOutcome().name());
        append(json, "state", event.state());
        append(json, "errorType", event.errorType());
        append(json, "reason", event.reason());
        append(json, "ingressQueueDepth", event.ingressQueueDepth());
        append(json, "windowQueueDepth", event.windowQueueDepth());
        append(json, "rankingQueueDepth", event.rankingQueueDepth());
        return json.append('}').toString();
    }

    private static void append(StringBuilder json, String name, String value) {
        if (value == null) return;
        separator(json);
        quote(json, name).append(':');
        quote(json, value);
    }

    private static void append(StringBuilder json, String name, Number value) {
        if (value == null) return;
        separator(json);
        quote(json, name).append(':').append(value);
    }

    private static void separator(StringBuilder json) {
        if (json.length() > 1) json.append(',');
    }

    private static StringBuilder quote(StringBuilder json, String value) {
        json.append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (character < 0x20) {
                        json.append(String.format("\\u%04x", (int) character));
                    } else {
                        json.append(character);
                    }
                }
            }
        }
        return json.append('"');
    }

    private static String sanitize(String value) {
        if (value == null) return null;
        String oneLine = value.replaceAll("[\\r\\n\\t]+", " ").trim();
        oneLine = CREDENTIAL.matcher(oneLine).replaceAll("$1=[REDACTED]");
        oneLine = JDBC_URL.matcher(oneLine).replaceAll("[REDACTED_DATABASE_URL]");
        oneLine = SQL.matcher(oneLine).replaceAll("[REDACTED_SQL]");
        oneLine = BASKET_DATA.matcher(oneLine).replaceAll("[REDACTED_BASKET_DATA]");
        oneLine = STACK_TRACE.matcher(oneLine).replaceAll(" [REDACTED_STACK_TRACE]");
        return oneLine.length() <= 256 ? oneLine : oneLine.substring(0, 256);
    }

    private static String sanitizeIdentifier(String value) {
        String sanitized = sanitize(value);
        return sanitized == null ? null : sanitized.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static String requiredText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
