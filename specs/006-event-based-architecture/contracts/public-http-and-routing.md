# Public HTTP and Routing Contract

**Requirements**: FR-016, FR-021–FR-024 | **Decisions**: D12 | **Base**: [005 public-http-and-routing.md](../../005-microservices-architecture/contracts/public-http-and-routing.md)

The seven routes, their handlers, their serialization, and the Nginx routing in `deploy/nginx.conf` are **unchanged**. `spec/self-checkout-openapi.yaml` and `load-client/src/**` stay byte-identical, and the existing hash checks remain (SC-001). This page records only how the new internal conditions map onto the established error contract.

| Internal condition | Route(s) affected | External response |
|---|---|---|
| Broker unavailable, slow, or refusing publishes | Complete, after the pre-checks pass | 500 `INTERNAL_ERROR` at the end of the 8 s budget; the attempt stays pending (FR-037) |
| Broker unavailable | Start, Scan, Lookup, `/items`, `/inventory/low-stock`, `/analytics/popular-items` | Unaffected |
| Outcome event not yet received (Inventory slow or down, lost event) | Complete | 500 `INTERNAL_ERROR` at the end of the budget |
| `StockRejected` with `INVALID_REQUEST`, `StockDeductionRefused`, or `StockRestored` while the request waits | Complete | 500 `INTERNAL_ERROR`; transaction OPEN |
| `StockRejected` with `INSUFFICIENT_STOCK` while the request waits | Complete | 409 `INSUFFICIENT_STOCK` naming the SKU |
| Pending attempt on the same transaction, still unsettled at the bound | Scan, Complete | 500 `INTERNAL_ERROR`, no change |
| No catalog replica yet | Scan | 500 `INTERNAL_ERROR` (FR-030) |
| Owning service down | Its routes | The gateway's existing 502 |

No response body names Kafka, a topic, an event type, or an offset. Error messages are those already produced by `ApiErrorMapper` (FR-016).
