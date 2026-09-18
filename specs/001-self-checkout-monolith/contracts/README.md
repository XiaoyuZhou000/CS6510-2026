# Contracts: Self-Checkout Monolithic Server

The authoritative API contract for this feature is
[`spec/self-checkout-openapi.yaml`](../../../spec/self-checkout-openapi.yaml) (OpenAPI 3.0.3). It
is **not reproduced here** — per FR-017 and Constitution III, it MUST NOT be modified or forked to
make this implementation pass, and duplicating it would risk the copy drifting out of sync with
the real contract used by `load-client/` and the grading harness.

This directory only adds contract detail that the OpenAPI file intentionally leaves open-ended:

- [`error-codes.md`](./error-codes.md) — the concrete `ApiError.error` string values this
  implementation returns for each documented `404`/`409`/`400` response, since the OpenAPI schema
  types `error` as a free-form string with only an illustrative example rather than an enum.

## Endpoint → implementation module map

For traceability between the fixed contract and the module boundaries in `plan.md`'s Project
Structure:

| Endpoint | Handler module | Notes |
|---|---|---|
| `GET /items` | `catalog` | read-only, `catalog_item` table |
| `POST /transactions` | `checkout` | one `transaction` row INSERT (status OPEN), creates in-memory `Basket` — no other endpoint touches the database this lightly |
| `POST /transactions/{id}/items` | `checkout`, `analytics` | purely in-memory: mutates `Basket`, records the scan into the analytics ring buffer; never touches the database itself (analytics persistence, when triggered, runs on a background executor — research.md §4) |
| `POST /transactions/{id}/complete` | `checkout`, `inventory` | see `research.md` §1 for the full transactional sequence; the only response this feature ever blocks on a DB commit for |
| `GET /transactions/{id}` | `checkout` | debugging only; reads in-memory `Basket` if OPEN, else falls back to the DB row + `transaction_line` |
| `GET /inventory/low-stock` | `inventory` | reads `inventory`, optional `threshold` query override |
| `GET /analytics/popular-items` | `analytics` | reads the `popular_window` row with the highest `window_id` (the latest successfully persisted checkpoint — research.md §4) + its `popular_item` rows |
