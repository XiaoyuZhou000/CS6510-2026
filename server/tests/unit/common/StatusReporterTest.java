package unit.common;

import metrics.StatusReporter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The FR-039 status line printer: its interval, its failure line, and its stop. */
class StatusReporterTest {
    @Test
    void printsTheSummaryEveryIntervalAndAnUnavailableLineWhenTheSummaryFails() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        AtomicInteger calls = new AtomicInteger();
        StatusReporter reporter = StatusReporter.start("probe", () -> {
            if (calls.incrementAndGet() == 2) throw new IllegalStateException("database is down: secret detail");
            return "status service=probe pending_work=no";
        }, Map.of("STATUS_INTERVAL_SECONDS", "1"), out);
        try {
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (calls.get() < 3 && System.nanoTime() < deadline) Thread.sleep(50);
        } finally {
            reporter.stop();
        }
        int printed = calls.get();
        assertTrue(printed >= 3, "one line per interval");
        String[] lines = buffer.toString(StandardCharsets.UTF_8).split("\\R");
        assertEquals("status service=probe pending_work=no", lines[0]);
        assertEquals("status service=probe unavailable error=IllegalStateException", lines[1],
                "the failure is named without its message");
        assertEquals("status service=probe pending_work=no", lines[2], "the next interval tries again");
        Thread.sleep(1_500);
        assertEquals(printed, calls.get(), "nothing is printed after stop");
        assertDoesNotThrow(reporter::stop, "stop is idempotent");
    }

    @Test
    void defaultsToThirtySecondsAndRejectsInvalidSettings() {
        assertEquals(30, StatusReporter.DEFAULT_INTERVAL_SECONDS);
        PrintStream out = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        StatusReporter quiet = StatusReporter.start("quiet", () -> "never yet", Map.of(), out);
        quiet.stop();
        assertThrows(IllegalArgumentException.class,
                () -> StatusReporter.start("bad", () -> "", Map.of("STATUS_INTERVAL_SECONDS", "0"), out));
        assertThrows(IllegalArgumentException.class, () -> StatusReporter.start(" ", () -> "", Map.of(), out));
        assertThrows(IllegalArgumentException.class, () -> StatusReporter.start("none", null, Map.of(), out));
    }
}
