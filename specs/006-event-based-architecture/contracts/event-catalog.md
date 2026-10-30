# Event Catalog

**Requirements**: FR-006–FR-016, FR-017–FR-020 | **Decisions**: D1, D3–D6, D9, D10 | **Schemas**: [proto/](proto/)

This is the message catalog FR-009 requires. Every inter-service interaction is listed here, and nothing else crosses a service boundary. The architecture diagram (D16) must match this page, and `ArchitectureDiagramConsistencyTest` checks that it does.

## Broker

One Apache Kafka 4.1.0 node in KRaft mode, at `127.0.0.1:19092` (D1). Topics are created only by `deploy/kafka/create-topics.ps1`, and `auto.create.topics.enable=false`.

## Topics

| Topic | Partitions | Cleanup and retention | Record key | Producer service | Consumer service (group or mode) | Consumer threads | Parked in |
|---|---:|---|---|---|---|---:|---|
| `selfcheckout.catalog.v1` | 1 | `compact`, infinite | SKU, or `__snapshot__` | Inventory | Transaction (manual assignment, replay from offset 0 at every start) | 1 | Transaction `dead_letter_event` |
| `selfcheckout.checkout.v1` | 16 | `delete`, 7 days | Transaction ID | Transaction | Inventory (`inventory-checkout`) | 8 | Inventory `dead_letter_event` |
| `selfcheckout.checkout-outcome.v1` | 16 | `delete`, 7 days | Transaction ID | Inventory | Transaction (`transaction-checkout-outcome`) | 8 | Transaction `dead_letter_event` |
| `selfcheckout.scan-accepted.v1` | 1 | `delete`, 7 days | Transaction ID | Transaction | Analytics (`analytics-scan-ingest`) | 1 | Analytics `dead_letter_event` |

The dead-letter destination is each consuming service's `dead_letter_event` table, the constitution's "equivalent parked state" (D6).

On the Windows broker (D1) every topic also pins `segment.bytes=1073741824` and `segment.ms=9223372036854775807`, so segments never roll and the cleanup policy and retention above never act: topics keep every record until the baseline reset. Consumers rely only on replay order and per-key last values, never on compaction or retention having run.

## Event types

Every type has `schema_version` 1 and exactly one owning producer.

| Event type | Topic | Producer | Consumer | Meaning (a recorded fact) | Payload | Idempotency key at the consumer |
|---|---|---|---|---|---|---|
| `CatalogItemPublished` | catalog | Inventory | Transaction | Inventory's catalog version V contains this SKU, name, and price | [catalog_events.proto](proto/catalog_events.proto) | `(sku, catalog_version)` |
| `CatalogSnapshotPublished` | catalog | Inventory | Transaction | Every item of version V has been published; V has `product_count` items | same | Replica version |
| `CheckoutRequested` | checkout | Transaction | Inventory | Transaction recorded checkout attempt n with these frozen lines | [checkout_events.proto](proto/checkout_events.proto) | `(transaction_id, attempt_number)` in `checkout_operation` |
| `StockRestorationRequested` (reserved) | checkout | Transaction (format owner; produced only by direct tests, D8) | Inventory | A restoration of attempt n's deduction was requested | same | `checkout_operation` state |
| `StockDeducted` | outcome | Inventory | Transaction | Inventory deducted all of attempt n's lines | same | Attempt state; guarded T3 |
| `StockRejected` | outcome | Inventory | Transaction | Inventory rejected attempt n, changing no stock, for `INSUFFICIENT_STOCK` or `INVALID_REQUEST`, naming the SKU | same | Attempt state; guarded reopen |
| `StockDeductionRefused` | outcome | Inventory | Transaction | Attempt n's stock had already been restored or blocked, so nothing was deducted | same | Attempt state; guarded reopen |
| `StockRestored` (reserved) | outcome | Inventory | Transaction | Inventory returned attempt n's deducted units | same | Attempt state; guarded reopen if pending |
| `RestorationNotNeeded` (reserved) | outcome | Inventory | Transaction | Inventory recorded the restoration request without returning units: `NOTHING_TO_RESTORE` (attempt now blocked) or `ALREADY_RESTORED` | same | No state change |
| `ScanAccepted` | scan-accepted | Transaction | Analytics | Transaction accepted one unit of this SKU into this transaction at this captured name and price | [scan_events.proto](proto/scan_events.proto) | `(source_transaction_id, source_scan_sequence)` |

## Envelope

The record value is a serialized `EventEnvelope` ([envelope.proto](proto/envelope.proto)), and the record key equals `routing_key`. Fields are described in research D3. Consumers validate before calling a handler:
- `event_id`, `event_type`, `routing_key`, and `producer` are present.
- `producer` is the type's owner.
- `schema_version` is supported.
- The payload decodes, and its identity fields match `routing_key`.

A failure of any check is permanent and parks the record (D6).

## Ordering guarantees (FR-012)

| Guarantee | How Kafka provides it |
|---|---|
| A transaction's `CheckoutRequested` and `StockRestorationRequested` events reach Inventory in the order they were recorded | Same key, so same partition. One dispatcher thread sends in commit order, and the idempotent producer keeps per-partition order across retries. Each partition is processed by one consumer thread at a time. |
| A transaction's outcome events reach Transaction in the order Inventory recorded them | Same as above, on the outcome topic |
| A catalog snapshot marker is read after every item of its version | One partition; items are written before the marker in one outbox transaction, and sent in `outbox_seq` order |
| Analytics sees one total order of scans | One partition. Positions follow offset order (D10). |
| No cross-key order is assumed | Every handler is correct under any interleaving of different keys |

A re-sent event (D5) can arrive after newer events for the same key. Handlers tolerate it because every effect is keyed to an attempt or version, and a settled attempt or older version is a no-op.

## Delivery semantics

| Property | Rule |
|---|---|
| Producer side | At least once from the outbox. Marked published only after `acks=all` confirmation (D5). |
| Consumer side | At least once. The offset is committed only after the local commit (D6). |
| Business effect | At most once per idempotency key above |
| Retries | Transient faults: indefinite, with backoff from 100 ms to 5 s, holding only the partition. Permanent faults: five attempts, then parked. |
| Producer configuration | `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `delivery.timeout.ms=30000`, `max.block.ms=1000`; `linger.ms` 0 for checkout and outcome events, 5 for catalog and scan events |
| Consumer configuration | `enable.auto.commit=false`, `auto.offset.reset=earliest`, `fetch.min.bytes=1`, `fetch.max.wait.ms=50`, `max.poll.records=500` |

## Catalog replication (FR-017–FR-020)

| Aspect | Rule |
|---|---|
| Source of truth | Inventory's `catalog_item`, versioned by `catalog_meta.catalog_version` |
| Publication | At every Inventory startup, one local transaction writes 2,000 `CatalogItemPublished` rows and one `CatalogSnapshotPublished` row to the outbox and sets `catalog_meta.published_at` (D9) |
| Initial load and late consumers | Transaction replays the compacted topic from offset 0 at every start. No request event is needed (FR-018). |
| Replacement | When a marker for a new version arrives and `product_count` distinct SKUs of that version were read, `product_replica` is replaced in one local transaction and the in-memory snapshot is swapped |
| Stale records | Records of other versions are ignored. One partition plus the version check means an older record can never overwrite a newer one. |
| Staleness bound | Under 5 s from Inventory's startup when healthy. The catalog is fixed between resets. |
| Source or broker unavailable | The durable replica keeps serving Scan, including across a Transaction restart (FR-017). No replica means Scan returns 500 (FR-030). |
| Analytics names | Carried in `ScanAccepted.product_name`, so Analytics keeps no catalog replica (FR-019) |

## Scan delivery

See [scan-event-delivery.md](scan-event-delivery.md).

## Checkout events

See [checkout-saga.md](checkout-saga.md).
