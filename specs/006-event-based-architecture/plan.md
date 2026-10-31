# Implementation Plan: Event-Based Self-Checkout Microservices with a Message Broker

**Branch**: `event-based-architecture` | **Date**: 2026-10-29 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/006-event-based-architecture/spec.md`. The setup script derived the name `006-event-based-architecture` from the feature directory. The checked-out Git branch and the spec header are `event-based-architecture`.

## Summary

Replace every synchronous call between the feature 005 microservices with domain events carried by one Apache Kafka 4.1.0 broker, then remove gRPC. Nothing changes for clients.

- **Broker**: one KRaft node run natively on Windows (D1, amended from Docker) with four topics (D4):
  - a compacted catalog topic;
  - a checkout topic and an outcome topic, keyed by transaction ID with 16 partitions;
  - a single-partition scan topic.
- **Outboxes everywhere** (D5): every event is written in its producer's database in the same local transaction as its cause. An `OutboxDispatcher` sends it immediately after the commit, a sweep re-sends anything unconfirmed, and confirmations are marked in batches. Every consumer commits its offset only after its local commit, is idempotent under a business key, and parks poison records in a `dead_letter_event` table (D6).
- **Choreographed checkout** (D7, D8):
  1. T1 records the attempt and `CheckoutRequested`.
  2. Inventory applies the deduction atomically and records `StockDeducted` or `StockRejected` in the same transaction.
  3. Transaction's outcome handler runs T3 or reopen and completes the waiting request's future.

  The 005 durable state machine, forward recovery, bounded waiting, and fault matrix carry over. Recovery re-sends the recorded request instead of calling an RPC. A compare-and-set responder claim keeps background-completion counts exact without an extra commit.
- **Catalog replication** (D9): Inventory republishes its full catalog through the outbox at every startup. Transaction replays the compacted topic from offset 0 and swaps its replica when a complete versioned snapshot arrives. No request event or synchronous call is needed.
- **Scans** (D10): the `accepted_scan` outbox and relay stay, now sending to Kafka. Analytics consumes them in one total order, assigns positions as in 005, and records its consumer offset in the same ingestion transaction.
- **Unchanged**: the seven REST routes and their handlers, the gateway, the Load Client, the per-service schemas and credentials, the guarded writes and in-memory projections, and the MySQL durability settings. Commits on the critical path stay at 005's N + 4 per basket (D15).
- **Deliverables**: two genuine reports from the event-based deployment and a Mermaid architecture diagram rendered by GitHub, with an SVG export (D16).

## Technical Context

**Language/Version**: Java 21 (`javac --release 21`); Protocol Buffers 3 for event schemas; SQL for MySQL 8.4; Bash build and run scripts; PowerShell database, broker, and benchmark tooling.

**Primary Dependencies**:
- JDK `HttpServer` for the unchanged REST handlers.
- JDBC with MySQL Connector/J 9.0.0, with 005's URL flags.
- `org.apache.kafka:kafka-clients:4.1.0`, `com.google.protobuf:protobuf-java:3.25.9`, and `org.slf4j:slf4j-simple:2.0.16`, pinned in the Maven `server/contracts` module (D2).
- JUnit Platform Console 1.11.0.
- Nginx as the gateway, unchanged.
- The Apache Kafka 4.1.0 binary release, run natively on the JDK (D1). Docker cannot run on the benchmark machine.
- **Removed**: every gRPC artifact.

**Storage**:
- One MySQL server (`127.0.0.1:3307`) with three schemas and three users, as in feature 005. New tables: `outbox_event` (Transaction and Inventory), `dead_letter_event` (all three), and `consumer_offset` (Analytics). New columns on `checkout_attempt`, `checkout_operation`, and `catalog_meta` ([data-model.md](data-model.md)).
- Kafka log storage in `KAFKA_DATA_DIR` on the host disk. Segments never roll and topics are never deleted, a Windows constraint (D1). The broker is never the sole record of business state.

**Testing**:
- Existing suites adapted, with the gRPC suites replaced through the coverage ledger.
- Deterministic Saga, catalog, and analytics fault suites on `InMemoryEventBus`.
- Live Kafka suites in `tests/messaging`: ordering, confirmation, offsets, parking, schema compatibility, broker `restart` and `kill`, direct restoration.
- Architecture tests: no synchronous calls between services, publishing only through the outbox, diagram consistency.
- Statement-budget and held-resource performance tests.
- Invariant validation and benchmark checkpoints CP0–CP4 with the unchanged Load Client.

**Target Platform**: Windows 10 with Git Bash and PowerShell 5.1, with Unix also supported. Transaction on HTTP 8081, Inventory on 8082, Analytics on 8083, Nginx on 8080, MySQL on 3307, and Kafka on `127.0.0.1:19092`, above the dynamic port range.

**Project Type**: Distributed web services: three microservices behind a static HTTP gateway, communicating only through an event broker.

**Performance Goals**:
- **Primary**: maximize the Load Client's `totalTransactions` in the default (10 stations, 60 s) and stress (100 stations, 120 s) workloads.
- **Comparison**: feature 005's CP4 evidence, re-measured as CP0 on this machine.
- **Also measured**: Complete's wait time, outbox commit-to-confirm, consumer lag per group, republishes, parks, and Analytics visibility.
- **Hard outcomes**:
  - zero errors, zero background completions, and zero parked events in the default run (SC-013);
  - Analytics visibility under 5 s at p99 when healthy, and every boundary published within 60 s after load (SC-008).
- **No numerical improvement target**.

**Constraints**:
- No synchronous call between services (FR-006).
- No publish without an outbox row (FR-010).
- Acknowledgement only after the local commit (FR-011).
- No connection, lock, or transaction held while waiting for an event (FR-040).
- Complete answers within an 8 s budget (FR-044).
- Server durability settings unchanged (`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`).
- Broker `acks=all` with an idempotent producer.
- REST contract and Load Client byte-identical.
- Connection budget of 122 unchanged.

**Scale/Scope**:
- 2,000 products at 10,000 units each.
- Up to about 280,000 accepted scans and about 12,000 checkouts per stress run.
- At most 100 concurrent baskets.
- Three services, one broker, four topics, ten event types, seven public routes, two committed reports, and one diagram.

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design.*

| Gate (Constitution v6.0.0) | Plan evidence | Result |
|---|---|---|
| **I. Independent microservices and domain ownership** | Three jars with their own entry points, ports, configuration, pools, and schemas. `common.jar` (messaging framework, metrics, faults) and `contracts.jar` (generated event classes) contain no business logic. Handlers live in each service's business layer, and Kafka types only in `messaging` adapters. The broker and the gateway are the only components besides the services. | PASS |
| **II. Database per service** | Unchanged three schemas and users. Cross-service data moves only by events. The product replica is fed only by Inventory's catalog events (D9) and has a documented source, ordering, and staleness. Outboxes, dead-letter tables, and Analytics' `consumer_offset` keep every acknowledged state in the owning database; the broker never holds the only copy. In-process queues, registries, and buffers are rebuildable optimizations. | PASS |
| **III. Event-based inter-service communication** | Every interaction is an event in the [event catalog](contracts/event-catalog.md). gRPC is removed, and an architecture test forbids synchronous clients. Version-controlled protobuf schemas with an envelope carrying ID, type, version, routing key, time, and correlation ID. One producer per type. Per-key ordering through partitioning. At-least-once delivery with idempotent consumers that acknowledge after commit. Bounded retries, then parking in `dead_letter_event`, without blocking other partitions. | PASS |
| **IV. External REST API and backward compatibility** | Routes, handlers, gateway, OpenAPI document, and Load Client are unchanged and hash-checked. Every endpoint stays synchronous; Complete waits for its outcome event within 8 s. Broker conditions map to the existing 500 or 502 ([public-http-and-routing.md](contracts/public-http-and-routing.md)). | PASS |
| **V. Distributed transactions and Saga coordination** | Choreography, chosen by the spec's clarification and justified in D7. Transaction durably owns the Saga state. Each step's events, local transaction, compensation (reopen) or retry rule (T3 after the pivot) are in [checkout-saga.md](contracts/checkout-saga.md). The progress and the outgoing event commit together. Inventory is idempotent per attempt, with the outcome event recorded in the same transaction. Restoration is reserved, durable, idempotent, and directly tested. Success is reported only after both commits. No two-phase commit across a database and the broker. | PASS |
| **VI. Checkout and inventory consistency** | 005's stock rules are unchanged: locking SELECT in SKU order, CHECK ≥ 0, unique effective deduction, captured prices. Pending attempts are durable and settle automatically once the broker and Inventory are reachable. Integrity faults (parked outcome or request, unknown attempt) keep the attempt pending with an alarm and never settle it on an assumed outcome. | PASS |
| **VII. Reliable event delivery and analytics accuracy** | Transactional outboxes for every event: `accepted_scan`, `outbox_event`, and the catalog publication transaction. Relays retry and mark published only after `acks=all` confirmation. Analytics deduplicates by scan identity and keeps `consumer_offset` in its ingestion transaction; broker offsets are only an optimization. Window, Ranking, and Persistence stages are unchanged. Broker or Analytics outages never block Scan. | PASS |
| **VIII. Performance and operational simplicity** | Exactly one broker product, with the choice, guarantees, persistence, and rejected alternatives in D1. Durable storage with `acks=all`. A residual page-cache risk on host loss is documented, with a measured flush option. No other infrastructure, and no container runtime: the broker is one native JVM. The broker and every service document build or install, startup, ports, configuration, storage, and topics ([broker-and-deployment.md](contracts/broker-and-deployment.md)). Restarting any one service or the broker is tested. | PASS |
| **IX. Verification and benchmark evidence** | Tests for database isolation, schema compatibility, API compatibility, concurrent checkout invariants, Saga success, insufficient stock (end-to-end compensation), duplicate and redelivered events, crash and restart at each step, broker restart and unavailability, outbox-only publishing, no synchronous calls, analytics loss and duplication, and direct restoration, reported separately. Two genuine reports from a reproducible baseline that includes the broker. A committed diagram with the gateway, services, databases, broker, and topics with their producers and consumers. | PASS |
| **Architectural constraints** | Incremental evolution of feature 005 (code reuse map below). gRPC interfaces are replaced and then removed. Endpoint ownership is unchanged. The replica is fed by Inventory's events, including its initial load. Per-service resets plus a broker topic reset in one baseline. The 2,000 × 10,000 baseline is preserved. | PASS |
| **Workflow and quality gates** | Ordering keys, idempotency keys, retry and dead-letter policies, Saga states and recovery, synchronous-endpoint wait and timeout behavior, replica consistency, consumption checkpoints, and visibility delay are defined in D4–D11 and the contracts. Every *Planning Obligations* item is traced below. | PASS |

**Prerequisite outside this plan**: constitution v6.0.0 is still an uncommitted working-tree change with its Sync Impact Report. Governance requires removing the report before the amendment is committed. The first task of `/speckit-tasks` does that and commits the constitution before or with this feature's first commit.

**Post-design re-check**: [data-model.md](data-model.md), the five contracts, the [proto baselines](contracts/proto/), and [quickstart.md](quickstart.md) preserve every gate above. No NEEDS CLARIFICATION remains and no deviation exists.

## Planning Obligations Traceability

| Spec obligation | Where it is resolved |
|---|---|
| 1. Broker selection | D1; [broker-and-deployment.md](contracts/broker-and-deployment.md#broker-install-start-stop-reset) |
| 2. Message catalog and schemas | D3, D4; [event-catalog.md](contracts/event-catalog.md); [proto/](contracts/proto/) |
| 3. Outbox relays and consumers | D5, D6; [data-model.md](data-model.md) (`outbox_event`, `dead_letter_event`) |
| 4. Saga over domain events | D7, D8, D11; [checkout-saga.md](contracts/checkout-saga.md) |
| 5. Catalog replication | D9; [event-catalog.md](contracts/event-catalog.md#catalog-replication-fr-017fr-020) |
| 6. Analytics delivery and ordering | D10; [scan-event-delivery.md](contracts/scan-event-delivery.md) |
| 7. Migration path | *Incremental Evolution Path* and *Code reuse map* below; D14 (coverage ledger, revised architecture test) |
| 8. Evidence | D13, D16; [broker-and-deployment.md](contracts/broker-and-deployment.md#benchmark-harness); [quickstart.md](quickstart.md) §6–7 |

## Phase 0: Research

All decisions, with rationale and alternatives, are in [research.md](research.md):
- D1: broker, Kafka 4.1.0 run natively on Windows.
- D2: build and dependencies.
- D3: envelope and schema evolution.
- D4: topics, keys, and partitions.
- D5: outbox and publication.
- D6: consumers and parking.
- D7: choreographed Saga.
- D8: Inventory handling and restoration.
- D9: catalog replication.
- D10: scan delivery.
- D11: waiting and multiple processes.
- D12: failure mapping.
- D13: baseline reset.
- D14: testing.
- D15: performance and checkpoints.
- D16: evidence and diagram.
- D17: deferred options.

## Phase 1: Design and Contracts

| Artifact | Content |
|---|---|
| [data-model.md](data-model.md) | Table changes per schema, event records and idempotency keys, projection changes, outbox and dead-letter state machines, validation rules |
| [contracts/event-catalog.md](contracts/event-catalog.md) and [contracts/proto/](contracts/proto/) | Topics, event types, producers, consumers, envelope, ordering, delivery semantics, catalog replication |
| [contracts/checkout-saga.md](contracts/checkout-saga.md) | Choreographed steps, outcome-handler rules, responder claim and response mapping, republish and recovery, fault matrix, fault hooks |
| [contracts/scan-event-delivery.md](contracts/scan-event-delivery.md) | Relay to Kafka, Analytics consumer with database offsets, ordering rule, visibility |
| [contracts/broker-and-deployment.md](contracts/broker-and-deployment.md) | Units, broker operation, configuration, baseline reset, status, harness, diagram |
| [contracts/public-http-and-routing.md](contracts/public-http-and-routing.md) | Unchanged routes; mapping of broker conditions |
| [quickstart.md](quickstart.md) | Broker start, build, baseline, smoke, suites, scenarios A–H, evidence, diagram |

### Key flows and statement budgets

These are design budgets, enforced as measured ceilings in `tests/performance/statement-budgets.properties`. They may rise only with A/B evidence.

| Flow | Warm-path work | Round trips |
|---|---|---|
| Start | Unchanged | 2 |
| Scan | Unchanged: guarded UPDATE + INSERT `accepted_scan` + commit | 3 |
| Complete (request thread) | Pre-checks from the cache; T1 = guarded UPDATE + INSERT attempt + INSERT outbox + commit; then wait | 4, plus the wait |
| Inventory apply (consumer) | Locking SELECT + INSERT operation + CASE UPDATE + INSERT outbox + commit | 5 |
| T3 or reopen (Transaction consumer) | Guarded UPDATE + UPDATE attempt + commit | 3 |
| Outbox marking, per batch (≤ 200 IDs or 20 ms) | One UPDATE + commit | 2 |
| Scan relay, per batch of ≤ 500 | SELECT + Kafka batch send + UPDATE + commit | 3 + send |
| Analytics ingestion, per batch | 005's 5–6 plus one `consumer_offset` UPSERT | 6–7 |

## Performance Strategy

**Cost model (D15)**:
- Durable commits on the critical path per basket stay at N + 4: Start, N Scans, T1, Inventory, T3. The outbox rows ride inside existing transactions.
- The new cost is latency: two broker hops of about 2–5 ms each, plus consumer fetch time.
- Marking commits are batched off the critical path.

**Optimizations**, in order:
1. **Immediate dispatch after commit** (D5) instead of polling, on both checkout hops.
2. **`linger.ms=0`** for checkout and outcome events; **`fetch.max.wait.ms=50`** with `fetch.min.bytes=1` for consumers.
3. **Parallel consumers**: eight consumer threads per checkout group, on 16 partitions.
4. **Batched marking** (20 ms) and **batched scan relay** (≤ 500 per send).
5. **005's memory-first projections** (state cache, catalog snapshot, name map) kept unchanged.

**Measurement**: `PERF_METRICS` timers for outbox commit-to-send and send-to-confirm, consume lag, handler time, Complete wait, republishes, parks, and marking batch size. The harness samples group lag every 2 s.

**Checkpoints** (005 R22 method, with the streamlined policy):
- **CP0**: feature 005 at `8891c57` on this machine.
- **CP1**: Saga over events with gRPC catalog and scans still present.
- **CP2**: full event-based system.
- **CP3**: A/B of `KAFKA_FLUSH_EVERY_MESSAGE`, consumer threads, scan linger, and outbox coalescing.
- **CP4**: official runs, designated in advance.

A drop of more than 5% from the previous checkpoint is investigated before continuing. Results are never claimed from latency or statement counts alone.

**Deferred options** (D17): Inventory group commit, immediate scan publishing, outbox purge, and flush-per-message as the default. Each needs its stated measured trigger.

## Incremental Evolution Path

Each stage leaves a buildable tree with passing tests. `/speckit-tasks` turns these stages into tasks.

0. **Governance and CP0**: remove the constitution's Sync Impact Report and commit v6.0.0. Run CP0 on `8891c57`.
1. **Broker and build**:
   - `deploy/kafka/` (broker configuration, start, stop, create, reset, and status scripts, README);
   - the event `.proto` files in `server/contracts` alongside the gRPC ones for now;
   - `kafka-clients` in `lib/messaging/`;
   - the Kafka-free `common/events` framework (ports, envelope codec, `OutboxDispatcher`, `ConsumerLoop`, parking) and the `common/messaging` Kafka adapters (producer and consumer factory);
   - `tests/support/InMemoryEventBus`;
   - `FaultHooks` and `ResponseBudget` moved out of `common/rpc` into `common/faults` and `common/time` without behavior changes (tasks.md T023), so the new framework and tests never import `rpc`;
   - schema baselines and compatibility tests.
2. **Schemas**: `outbox_event`, `dead_letter_event`, `consumer_offset`, and the new columns, through `db/<service>/schema.sql` and resets.
3. **Checkout over events** (CP1, taken once the Saga runs over events and before the fault and wait suites migrate, because those add no hot-path work; tasks.md T079):
   - Inventory's `CheckoutRequestHandler` and `StockRestorationHandler` with outcome outbox rows;
   - Transaction's T1 outbox row, `CheckoutOutcomeHandler`, responder claim, and republishing recovery worker;
   - removal of `GrpcInventoryGateway.applyCheckout` use;
   - the Saga, fault, and wait suites migrated to the in-memory bus and live Kafka.
4. **Catalog over events**: Inventory's `CatalogPublisher`, Transaction's `CatalogConsumer`, and removal of `CatalogReplicaRefresher`'s gRPC polling.
5. **Scans over events** (CP2): `KafkaScanEventSink`, Analytics' `ScanAcceptedConsumer` with `consumer_offset`, and removal of `AnalyticsIngestGrpcService`.
6. **gRPC removal**:
   - delete the `rpc` packages, the gRPC `.proto` files, the gRPC dependencies, `lib/grpc/`, and the gRPC configuration;
   - revise `MicroserviceBoundaryTest`, `LayerDependencyTest`, and `CompositionRootTest`;
   - add `OutboxOnlyPublishTest`;
   - complete the coverage ledger before deleting `tests/rpc`.
7. **Tooling and documentation**:
   - `db/reset-baseline.ps1` and the extended `validate-invariants.ps1`;
   - `server/benchmark-event-based.ps1` and the `benchmark-compare.ps1` baseline switch;
   - the status lines;
   - `server/README.md` and `deploy/kafka/README.md`;
   - the diagram and `ArchitectureDiagramConsistencyTest`.
8. **CP3 tuning A/B, then CP4**: the official default and stress runs, the report copies, the comparison README, and the quickstart validation record.

### Code reuse map

| Existing code | Plan |
|---|---|
| All HTTP handlers, `ApiErrorMapper` | Unchanged |
| `TransactionService`, `TransactionStateCache`, `TransactionLocks`, `TransactionStates` | Unchanged except for the Complete wiring |
| `CheckoutOrchestrator` | Adapted: T1 also writes the outbox row and signals the dispatcher; the request waits on the registry instead of calling `InventoryGateway`; the in-request retry loop and RPC deadline are removed |
| `SettlementRegistry` | Adapted: responder state with compare-and-set claim (D7) |
| `CheckoutRecoveryWorker`, `RecoverySchedule` | Adapted: re-send the recorded `CheckoutRequested`; never settle locally; stuck alarm |
| `InventoryGateway`, `GrpcInventoryGateway`, `DependencyUnavailableFailure` (catalog uses) | Removed; replaced by `CheckoutOutcomeHandler`, `CatalogEventHandler`, and the outbox |
| `CatalogReplicaRefresher` | Replaced by `CatalogEventHandler` (business) and `CatalogConsumer` (messaging); `ProductCatalogReplica` and `JdbcProductReplicaStore` reused |
| `ScanEventRelay`, `ScanEventSink`, `JdbcRelayStore` | Reused; the sink becomes `KafkaScanEventSink`; the column is renamed `published_at` |
| `GrpcAnalyticsIngestClient`, `AnalyticsIngestGrpcService`, `InventoryGrpcService` | Removed |
| `JdbcCheckoutAttemptStore` | Adapted: the T1 outbox insert, `request_event_id`, and the responder-dependent `client_outcome` |
| `CheckoutStockService`, `JdbcCheckoutOperationStore` | Adapted: the outcome outbox insert inside the apply and restore transactions; recorded-event lookup for duplicates |
| `CatalogCache`, `JdbcCatalogMetaStore` | Reused; `catalog_meta.published_at` added; `CatalogPublisher` added |
| `ScanIngestionService`, `JdbcScanIngestStore` | Adapted: `consumer_offset` UPSERT in the ingestion transaction |
| Analytics pipeline (`AcceptedScanPoller`, `WindowFilter`, `RankingFilter`, `PersistenceFilter`, …), `JdbcPopularWindowStore` | Unchanged |
| `common/rpc/FaultHooks`, `ResponseBudget` | Moved to `common/faults` and `common/time`; `GrpcChannels`, `GrpcServers`, and `RpcStatusClassifier` removed |
| `common/metrics`, `common/database`, `common/api` | Reused; `StatusReporter` gains the new fields |
| `*StatusQueries` | Extended with outbox, parked, lag, and replica fields |
| `server/contracts` | Reused as the event-contract module (D2) |
| `db/*` scripts, `db/<service>/*.sql` | Extended: new tables and columns, `reset-baseline.ps1`, extended validation |
| `server/benchmark-microservices.ps1`, `benchmark-compare.ps1` | `benchmark-event-based.ps1` added; compare gains the 005 baseline worktree |
| `server/tests/rpc/*`, `GrpcTestSupport`, `FakeInventoryGateway` | Replaced by `tests/messaging/*`, `InMemoryEventBus`, and `FakeInventoryReactor`, recorded per method in `COVERAGE-LEDGER.md` (FR-065) |
| Other `server/tests/**` | Compatible tests kept; the rest migrated without weaker assertions |

## Project Structure

### Documentation (this feature)

```text
specs/006-event-based-architecture/
├── spec.md
├── plan.md                         # this file
├── research.md                     # Phase 0
├── data-model.md                   # Phase 1
├── quickstart.md                   # Phase 1
├── contracts/                      # Phase 1
│   ├── event-catalog.md
│   ├── checkout-saga.md
│   ├── scan-event-delivery.md
│   ├── broker-and-deployment.md
│   ├── public-http-and-routing.md
│   └── proto/
│       ├── envelope.proto
│       ├── catalog_events.proto
│       ├── checkout_events.proto
│       └── scan_events.proto
├── checklists/requirements.md
└── tasks.md                        # Phase 2, produced by /speckit-tasks
```

### Source code (repository root)

```text
server/
├── contracts/                      # Maven: event .proto → out/contracts.jar; runtime deps → lib/messaging/ (git-ignored)
│   ├── pom.xml                     # protobuf-java 3.25.9, kafka-clients 4.1.0, slf4j-simple 2.0.16; no gRPC
│   └── src/main/proto/selfcheckout/events/v1/{envelope,catalog_events,checkout_events,scan_events}.proto
├── common/src/
│   ├── api/  database/  metrics/   # reused
│   ├── events/                     # NEW, Kafka-free: EnvelopeCodec, OutboxDispatcher, ConsumerLoop, ParkingPolicy, TopicNames,
│   │                               #      ports OutboxStore, OutboxSignal, EventHandler, DeadLetterStore, RecordSender, RecordStream
│   ├── messaging/                  # NEW, Kafka adapters only: KafkaClients, KafkaRecordSender, KafkaRecordStream
│   ├── faults/FaultHooks.java      # moved from rpc/
│   └── time/ResponseBudget.java    # moved from rpc/
├── transaction/src/
│   ├── api/                        # unchanged
│   ├── transaction/                # TransactionService, CheckoutOrchestrator, CheckoutOutcomeHandler, CatalogEventHandler,
│   │                               # CheckoutRecoveryWorker, SettlementRegistry, ScanEventRelay, … TransactionMain
│   ├── database/                   # + JdbcOutboxStore, JdbcDeadLetterStore; adapted attempt, relay, and replica stores
│   └── messaging/                  # NEW: CheckoutOutcomeConsumer, CatalogConsumer, KafkaScanEventSink
├── inventory/src/
│   ├── api/                        # unchanged
│   ├── inventory/                  # StoreQueryService, CatalogCache, CheckoutStockService, CheckoutRequestHandler,
│   │                               # StockRestorationHandler, CatalogPublisher, InventoryMain
│   ├── database/                   # + JdbcOutboxStore, JdbcDeadLetterStore; adapted operation and catalog-meta stores
│   └── messaging/                  # NEW: CheckoutEventConsumer
├── analytics/src/
│   ├── api/                        # unchanged
│   ├── analytics/                  # existing pipeline + ScanIngestionService (adapted), AnalyticsMain
│   ├── database/                   # + consumer_offset in JdbcScanIngestStore, JdbcDeadLetterStore
│   └── messaging/                  # NEW: ScanAcceptedConsumer
├── tests/
│   ├── COVERAGE-LEDGER.md          # extended for the gRPC → event migration (FR-065)
│   ├── unit/ architecture/ database/ contract/ integration/ performance/   # existing, adapted
│   ├── messaging/                  # NEW: schema compatibility (+ baseline/), live Kafka ordering, confirmation, offsets,
│   │                               #      parking, broker restart, RestorationEventDirectTest
│   └── support/                    # + InMemoryEventBus, FakeInventoryReactor, KafkaTestSupport, PublishRawRecord
├── lib/                            # Connector/J, JUnit (committed); messaging/ (Maven-resolved, git-ignored)
├── build.sh  build-tests.sh  run-tests.sh  run-transaction.sh  run-inventory.sh  run-analytics.sh
├── benchmark-event-based.ps1       # NEW harness
└── benchmark-compare.ps1           # interleaved A/B, with the 005 baseline worktree

deploy/
├── nginx.conf                      # unchanged
└── kafka/                          # NEW: server.properties, start/stop-broker.ps1, create-topics.ps1, reset-topics.ps1, status.ps1, README.md

db/
├── bootstrap.ps1  reset-service.ps1  reset-all.ps1  validate-invariants.ps1   # extended
├── reset-baseline.ps1              # NEW: broker topics + all services (D13)
└── transaction/ inventory/ analytics/   # schema.sql (+ outbox, dead-letter, offsets), seed.sql, validate.sql

docs/architecture/
├── event-based-architecture.md     # NEW Mermaid diagram (renders on GitHub)
└── event-based-architecture.svg    # NEW export

load-client/                        # unchanged source
└── reports/event-based-architecture/   # checkpoints, default-report.json, stress-report.json, README.md
```

**Structure Decision**:
- **Layering**: keep the three ownership roots with their `api`, business, and `database` layers. Replace each service's `rpc` adapter layer with a `messaging` adapter layer. Event handlers are business classes behind the `EventHandler` port, and Kafka types never leave `messaging`.
- **Shared code**: the outbox dispatcher and consumer loop are shared, Kafka-free infrastructure in `common/events`, which business code may import; the Kafka adapters behind its `RecordSender` and `RecordStream` ports live in `common/messaging`, which business code never imports. Storage sits behind ports implemented in `common/database` (tasks.md Implementation Rule 5). No business logic moves into `common`.
- **Broker files**: everything the broker needs lives in `deploy/kafka/`, next to the gateway configuration.

## Complexity Tracking

> No constitution violation exists, so the violation table is empty.

**Non-violation complexity notes**:
- **Native Kafka on Windows**: Kafka does not officially support Windows, and Docker cannot run on the benchmark machine (D1). The broker runs as a native JVM under Windows file-lock rules: segments never roll, topics are never deleted, and the baseline reset replaces the log directory. The services stay plain JARs.
- **Outbox dispatcher with immediate send plus sweep**: two publication paths, justified by removing two polling delays from every Complete. The sweep alone is sufficient for correctness, and tests disable the immediate path to prove it.
- **Responder claim in the settlement registry**: one compare-and-set state per pending attempt, justified by keeping the FR-061 counts exact without adding a commit to every successful Complete. The registry stays process-local, and durable state decides every outcome.
- **The dead-letter table instead of dead-letter topics**: fewer topics, durable across broker resets, and queryable with each service's own credentials.
- **Deferred options**: listed in D17 with measured entry criteria, so speculative complexity is not added without evidence.

## Implementation Notes

The T173 constitution compliance review (2026-10-31) re-checked each row of the *Constitution Check* table against the code and the evidence. Every gate still passes. The architecture suite enforces gates I–IV (`LayerDependencyTest`, `MicroserviceBoundaryTest`, `OutboxOnlyPublishTest`, `CompositionRootTest`, `ArchitectureDiagramConsistencyTest`). The release suite, the quickstart scenarios, and the CP4 evidence carry gates V–IX (`load-client/reports/event-based-architecture/`). The implementation refined the plan in these places, none of which changes a gate:

- **Shared JDBC adapters** (Implementation Rule 5): `outbox_event` and `dead_letter_event` have one shape in every schema. Their adapters, `JdbcOutboxStore` and `JdbcDeadLetterStore`, live once in `server/common/src/database/`, and each service instantiates them with its own pool. The plan listed them per service, but identically named per-service classes would shadow each other on the test classpath. Each service still reaches only its own schema with its own credentials (gate II).
- **`events` and `messaging` packages**: the event framework (ports, `EnvelopeCodec`, `OutboxDispatcher`, `ConsumerLoop`, `TopicNames`, `EventTypes`) is Kafka-free in `common/src/events/`, and business code may import it. Kafka types appear only in `common/src/messaging/` and `<service>/src/messaging/`, which only the `*Main` composition roots wire (gate I, `LayerDependencyTest`).
- **Lazy producers**: `KafkaRecordSender.lazy` creates each producer on its first send, so every service starts while the broker is unreachable (US2, US7). No request waits on the broker for more than `max.block.ms` (1 s).
- **Shutdown order**: HTTP, then the consumers (with a final offset commit), the outbox dispatcher (with a final mark), the background workers, the producers, and the pool. `CompositionRootTest` checks it.
- **Public error mapping** (T143): `TransactionHttpHandler` answers 400 `INVALID_REQUEST` only for client input. An internal `IllegalArgumentException` is a sanitized 500 (gate IV).
- **Native broker** (D1): Kafka cannot delete a topic on Windows without taking its log directory offline. So `delete.topic.enable=false`, segments never roll, tests use per-run topic prefixes, and `reset-topics.ps1` replaces the whole log directory for the baseline (gate VIII).
- **CP3 adoptions**: none. Flushing on every message cost 29.9% at default and 16.1% under stress, so D17's ±3% criterion was not met and the broker keeps its page-cache default, with `acks=all` and the outboxes providing durability. Four consumer threads (+2.2%, +4.1%), a 20 ms scan linger (−3.2%), and 100 ms relay coalescing (−4.2%) did not reach the +5% bar. Inventory group commit met its D17 criterion at CP2 but was not implemented, and it stays deferred.
- **Benchmark harness**: `benchmark-compare.ps1` reads git's whole output before taking the first line. `| Select-Object -First 1` stopped git early and left `$LASTEXITCODE` at −1. The idle gate's blocking list gained `Arknights`, which loaded CP2.
- **Outcome**: against feature 005 at `8891c57`, measured interleaved (T166), `totalTransactions` is 7.5% lower at default and 6.1% lower under stress. That is the accepted, explained cost of durable broker hops (spec Assumptions); correctness was not traded for it.
