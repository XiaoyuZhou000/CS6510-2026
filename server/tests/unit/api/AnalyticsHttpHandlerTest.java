package unit.api;

import analytics.AnalyticsOperations;
import api.AnalyticsHttpHandler;
import database.StoreFailure;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsHttpHandlerTest {
    @Test
    void defaultsToTenAndSerializesTheEmptyPersistedView() throws Exception {
        FakeAnalytics analytics = new FakeAnalytics();
        TestExchange exchange = TestExchange.request("GET", "/analytics/popular-items", "");

        new AnalyticsHttpHandler(analytics).handle(exchange);

        assertEquals(200, exchange.status());
        assertEquals(List.of(10), analytics.limits);
        assertEquals(0, ((Number) exchange.json().get("windowStart")).longValue());
        assertEquals(List.of(), exchange.json().get("items"));
    }

    @Test
    void delegatesExplicitLimitsIncludingZeroAndSerializesLatestRanks() throws Exception {
        FakeAnalytics analytics = new FakeAnalytics();
        analytics.view = new AnalyticsOperations.PopularItemsView(1000, 500, 501, 1500,
                Instant.parse("2026-09-23T12:00:00Z"), List.of(
                new AnalyticsOperations.RankedItemView("A", "Apple", 700, 1),
                new AnalyticsOperations.RankedItemView("B", "Bread", 300, 2)));
        AnalyticsHttpHandler handler = new AnalyticsHttpHandler(analytics);

        TestExchange explicit = TestExchange.request(
                "GET", "/analytics/popular-items?limit=2", "");
        handler.handle(explicit);
        TestExchange zero = TestExchange.request(
                "GET", "/analytics/popular-items?limit=0", "");
        handler.handle(zero);

        assertEquals(List.of(2, 0), analytics.limits);
        assertEquals(1500, ((Number) explicit.json().get("windowEnd")).longValue());
        assertEquals(2, ((List<?>) explicit.json().get("items")).size());
        assertEquals(200, zero.status());
    }

    @Test
    void rejectsNegativeMalformedAndEmptyLimitsWithoutDelegation() throws Exception {
        FakeAnalytics analytics = new FakeAnalytics();
        AnalyticsHttpHandler handler = new AnalyticsHttpHandler(analytics);

        for (String limit : List.of("-1", "abc", "")) {
            TestExchange exchange = TestExchange.request(
                    "GET", "/analytics/popular-items?limit=" + limit, "");
            handler.handle(exchange);
            assertEquals(400, exchange.status());
            assertEquals("INVALID_LIMIT", exchange.json().get("error"));
        }
        assertTrue(analytics.limits.isEmpty());
    }

    @Test
    void mapsFailuresWithoutExposingPersistenceDetails() throws Exception {
        FakeAnalytics analytics = new FakeAnalytics();
        analytics.failure = new StoreFailure("SELECT failed at private host");
        TestExchange exchange = TestExchange.request("GET", "/analytics/popular-items", "");

        new AnalyticsHttpHandler(analytics).handle(exchange);

        assertEquals(500, exchange.status());
        assertEquals("Internal server error", exchange.json().get("message"));
        assertFalse(exchange.body().contains("SELECT"));
    }

    private static final class FakeAnalytics implements AnalyticsOperations {
        private final List<Integer> limits = new ArrayList<>();
        private PopularItemsView view = PopularItemsView.empty();
        private RuntimeException failure;

        @Override public void recordAcceptedScan(String sku) { fail(); }

        @Override
        public PopularItemsView latestPopularItems(int limit) {
            limits.add(limit);
            fail();
            return view;
        }

        @Override public void shutdown() { }

        private void fail() {
            if (failure != null) throw failure;
        }
    }
}
