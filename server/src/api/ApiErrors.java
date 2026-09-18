package api;

import json.Json;

public final class ApiErrors {

    public static final String MISSING_STATION_ID    = "MISSING_STATION_ID";
    public static final String TRANSACTION_NOT_FOUND = "TRANSACTION_NOT_FOUND";
    public static final String SKU_NOT_FOUND         = "SKU_NOT_FOUND";
    public static final String TRANSACTION_NOT_OPEN  = "TRANSACTION_NOT_OPEN";
    public static final String EMPTY_BASKET          = "EMPTY_BASKET";
    public static final String INSUFFICIENT_STOCK    = "INSUFFICIENT_STOCK";

    private ApiErrors() {}

    /** Serializes an ApiError to JSON: {"error":"...","message":"..."}. */
    public static String toJson(String errorCode, String message) {
        return Json.object(
            "error",   Json.quote(errorCode),
            "message", Json.quote(message)
        );
    }
}
