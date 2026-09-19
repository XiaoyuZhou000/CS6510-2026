package unit;

import api.ApiErrors;
import json.Json;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JsonRoundTripTest {
    @Test
    void roundTripsEveryResponseDtoShape() {
        assertRoundTrip(Json.object(
            "sku", Json.quote("SKU-\"001\\A"),
            "name", Json.quote("Tea\nGreen"),
            "price", Json.number(new BigDecimal("12.30"))),
            Map.of("sku", "SKU-\"001\\A", "name", "Tea\nGreen", "price", 12.30));

        assertRoundTrip(Json.object(
            "transactionId", Json.quote("tx-1"),
            "stationId", Json.quote("station-1"),
            "status", Json.quote("OPEN"),
            "itemCount", Json.number(2),
            "runningTotal", Json.number(new BigDecimal("24.60")),
            "startedAt", Json.quote("2026-09-18T01:02:03Z")),
            Map.of("transactionId", "tx-1", "stationId", "station-1", "status", "OPEN",
                "itemCount", 2.0, "runningTotal", 24.60, "startedAt", "2026-09-18T01:02:03Z"));

        assertRoundTrip(Json.object(
            "transactionId", Json.quote("tx-1"),
            "sku", Json.quote("SKU-1"),
            "name", Json.quote("Tea"),
            "unitPrice", Json.number(new BigDecimal("12.30")),
            "itemCount", Json.number(2),
            "runningTotal", Json.number(new BigDecimal("24.60"))),
            Map.of("transactionId", "tx-1", "sku", "SKU-1", "name", "Tea",
                "unitPrice", 12.30, "itemCount", 2.0, "runningTotal", 24.60));

        String receiptLine = Json.object(
            "sku", Json.quote("SKU-1"), "name", Json.quote("Tea"),
            "unitPrice", Json.number(new BigDecimal("12.30")), "quantity", Json.number(2));
        Map<String, Object> receipt = Json.parseObject(Json.object(
            "transactionId", Json.quote("tx-1"), "stationId", Json.quote("station-1"),
            "itemCount", Json.number(2), "totalAmount", Json.number(new BigDecimal("24.60")),
            "startedAt", Json.quote("2026-09-18T01:02:03Z"),
            "completedAt", Json.quote("2026-09-18T01:03:03Z"),
            "lines", Json.array(List.of(receiptLine))));
        assertEquals(2, ((Number) receipt.get("itemCount")).intValue());
        assertEquals("SKU-1", Json.asObject(Json.getList(receipt, "lines").getFirst()).get("sku"));

        assertRoundTrip(Json.object(
            "sku", Json.quote("SKU-2"), "name", Json.quote("Milk"),
            "currentStock", Json.number(3), "threshold", Json.number(3),
            "triggeredAt", Json.quote("2026-09-18T01:04:03Z")),
            Map.of("sku", "SKU-2", "name", "Milk", "currentStock", 3.0,
                "threshold", 3.0, "triggeredAt", "2026-09-18T01:04:03Z"));

        String popularItem = Json.object(
            "sku", Json.quote("SKU-3"), "name", Json.quote("Bread"),
            "scanCount", Json.number(99L), "rank", Json.number(1));
        assertRoundTrip(popularItem,
            Map.of("sku", "SKU-3", "name", "Bread", "scanCount", 99.0, "rank", 1.0));

        Map<String, Object> popular = Json.parseObject(Json.object(
            "windowSize", Json.number(1000), "slideInterval", Json.number(500),
            "windowStart", Json.number(501L), "windowEnd", Json.number(1500L),
            "computedAt", Json.quote("2026-09-18T01:05:03Z"),
            "items", Json.array(List.of(popularItem))));
        assertEquals(1000, ((Number) popular.get("windowSize")).intValue());
        assertEquals("SKU-3", Json.asObject(Json.getList(popular, "items").getFirst()).get("sku"));

        assertRoundTrip(ApiErrors.toJson(ApiErrors.TRANSACTION_NOT_OPEN, "Already completed"),
            Map.of("error", "TRANSACTION_NOT_OPEN", "message", "Already completed"));
    }

    private static void assertRoundTrip(String json, Map<String, ?> expected) {
        assertEquals(expected, Json.parseObject(json));
    }
}
