package unit.common;

import org.junit.jupiter.api.Test;
import rpc.ResponseBudget;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ResponseBudgetTest {
    @Test
    void remainingCountsDownOnTheMonotonicClockAndNeverGoesNegative() {
        AtomicLong clock = new AtomicLong(5_000_000_000L);
        ResponseBudget budget = ResponseBudget.start(Duration.ofSeconds(8), clock::get);
        assertEquals(5_000_000_000L, budget.startNanos());
        assertEquals(Duration.ofSeconds(8), budget.total());
        assertEquals(Duration.ofSeconds(8), budget.remaining());
        assertFalse(budget.expired());

        clock.addAndGet(Duration.ofMillis(7_500).toNanos());
        assertEquals(Duration.ofMillis(500), budget.remaining());
        assertFalse(budget.expired());

        clock.addAndGet(Duration.ofMillis(500).toNanos());
        assertEquals(Duration.ZERO, budget.remaining());
        assertTrue(budget.expired());

        clock.addAndGet(Duration.ofSeconds(3).toNanos());
        assertEquals(Duration.ZERO, budget.remaining());
        assertTrue(budget.expired());
    }

    @Test
    void nextDeadlineIsTheSmallerOfThePerCallDeadlineAndTheTimeLeft() {
        AtomicLong clock = new AtomicLong();
        ResponseBudget budget = ResponseBudget.start(Duration.ofSeconds(8), clock::get);
        assertEquals(Duration.ofSeconds(2), budget.nextDeadline(Duration.ofSeconds(2)));
        clock.set(Duration.ofMillis(6_750).toNanos());
        assertEquals(Duration.ofMillis(1_250), budget.nextDeadline(Duration.ofSeconds(2)));
        clock.set(Duration.ofSeconds(9).toNanos());
        assertEquals(Duration.ZERO, budget.nextDeadline(Duration.ofSeconds(2)));
    }

    @Test
    void realClockBudgetStartsFullAndRejectsNegativeTotals() {
        ResponseBudget budget = ResponseBudget.start(Duration.ofSeconds(8));
        assertTrue(budget.remaining().compareTo(Duration.ofSeconds(7)) > 0);
        assertTrue(ResponseBudget.start(Duration.ZERO).expired());
        assertThrows(IllegalArgumentException.class, () -> ResponseBudget.start(Duration.ofMillis(-1)));
    }
}
