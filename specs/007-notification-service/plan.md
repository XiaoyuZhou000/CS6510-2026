# Implementation Plan: Sale Notification Service with Subscriber Reconciliation

**Branch**: `feature/notification-service` | **Date**: 2026-11-11 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/007-notification-service/spec.md`. The setup script derived the name `007-notification-service` from the feature directory. The checked-out Git branch is `feature/notification-service`, based on `event-based-architecture` at `4a91686`.

## Summary

Add an independently deployable fourth service, **Notification Service**, to the feature 006 event-based system, plus a separate **subscriber test application** and an independent **reconciler**.

- **Completed sales** (N2, N3): T3, Transaction's completion transaction, also inserts one `SaleCompleted` outbox row. It has a deterministic event ID, is keyed by transaction ID, and is published through the existing `OutboxDispatcher` to a new 16-partition `selfcheckout.sale-completed.v1` topic. T3 grows from two statements to three and still makes one commit. Nothing else in checkout changes.
- **Subscriptions** (N4, N6): Notification owns subscribers, current subscriptions, and an ordered history of changes numbered by a single locked clock row. A change takes effect when its transaction commits, and the response carries its history version. SKUs are validated against a catalog set that Notification rebuilds by replaying Inventory's compacted catalog topic.
- **Selection** (N5): one local transaction per sale takes the clock row `FOR SHARE`, so the state it reads is exactly the state at version V. It selects recipients from an in-memory index that is checked against V and catches up from the history when behind. It then records `sale_selection` (with V) and one `notification` row per (sale, SKU, subscriber) before the offset is committed.
- **Delivery** (N7, N8):
  - each subscriber has its own single-partition channel topic, created by Notification;
  - the `notification` row is the outbox and the delivery record;
  - a dispatcher with per-subscriber lanes publishes with `acks=all`, marks rows delivered in batches, and retries failures with backoff;
  - after 8 failures charged to the channel itself, a row is parked and can be retried by an operator;
  - broker-wide failures are never charged, so a broker outage never parks notifications.
- **Verification** (N10–N12):
  - `subscriber-client/` registers seeded subscribers, runs one Kafka consumer thread per subscriber, and writes durable receipt records;
  - each owner exports its own data with its own credentials;
  - `ReconcileMain` compares completed sales, the subscription snapshot or recorded history, delivery records, and receipts, and fails on any mismatch;
  - the subscription-change scenario checks causality with history versions instead of clocks.
- **Evidence** (N15):
  - 4 subscriber counts (10, 25, 50, 100);
  - the default and stress Load Client reports with 100 subscribers, kept unmodified;
  - a separate notification report per run;
  - 9 isolation runs;
  - an updated diagram.

## Technical Context

**Language/Version**: Java 21 (`javac --release 21`); Protocol Buffers 3 for event schemas; SQL for MySQL 8.4; Bash build and run scripts; PowerShell database, broker, and benchmark tooling. This is unchanged from 006.

**Primary Dependencies**:
- Unchanged from 006: JDK `HttpServer`; MySQL Connector/J 9.0.0; `kafka-clients` 4.1.0, including the `Admin` client for channel topics; `protobuf-java` 3.25.9; `slf4j-simple` 2.0.16; JUnit Platform Console 1.11.0; Nginx; the native Apache Kafka 4.1.0 broker.
- No new third-party dependency.

**Storage**:
- MySQL server `127.0.0.1:3307` with four schemas and four users. New: `cs6510_notification` with `subscription_clock`, `subscriber`, `subscription`, `subscription_change`, `sale_selection`, `notification`, and `dead_letter_event` ([data-model.md](data-model.md)). Transaction's schema is unchanged; it gains a new `outbox_event` event type.
- Kafka log storage as in 006, with five fixed topics plus one channel topic per subscriber.
- The subscriber test application's files in a run directory.

**Testing**:
- 006 suites kept unchanged, plus:
  - unit tests (index, changes, selection, charging, reconciler, seed determinism);
  - database tests (clock locking, selection, rollback, isolation);
  - in-memory bus flow and crash-matrix tests;
  - live Kafka tests (schema compatibility, channels, isolation, broker restart, parking);
  - architecture tests (no recipient logic in Transaction, outbox-only publishing, four-service boundaries, diagram);
  - statement budgets;
  - quickstart scenarios A–H.

**Target Platform**: Windows 10 with Git Bash and PowerShell 5.1, with Unix also supported. Transaction on 8081, Inventory on 8082, Analytics on 8083, **Notification on 8084**, Nginx on 8080, MySQL on 3307, and Kafka on `127.0.0.1:19092`.

**Project Type**: Distributed web services: four microservices behind a static HTTP gateway, communicating only through one Kafka broker, plus two test clients (the unchanged Load Client and the new subscriber test application).

**Performance Goals**:
- **Checkout**: `totalTransactions` within the measured cost of the extra T3 INSERT; the design estimate is ≤ 3% below CP0, and correctness is never traded.
- **Notifications** at 100 subscribers in the default run:
  - every owed notification received within 60 s after the final completed sale (SC-007);
  - zero delivery failures when healthy;
  - p95 for the healthy 99 subscribers within 20% of healthy runs while one channel is faulted (SC-006).
- **Measured**: sales/s, delivered/s, received/s, latency p50/p95/p99 (completion to receipt, and completion to delivered), charged and uncharged failures, and backlog at four points.

**Constraints**:
- Every 006 constraint holds: no synchronous inter-service call, no publish without an outbox row, acknowledgement only after commit, no connection or lock held while waiting, Complete within its 8 s budget, unchanged durability settings, `acks=all`.
- Recipient selection never in Transaction (FR-004).
- No second broker (FR-009).
- The Load Client and the OpenAPI document byte-identical (SC-009).
- Connection budget 142 < 151.

**Scale/Scope**:
- 2,000 SKUs; 100 subscribers (tested from 10 to 100, and supported beyond) × 20 SKUs.
- About 4,400 sales and 35,000 notifications per default run; about 10,500 sales and 85,000 notifications per stress run.
- Four services, one broker, five fixed topics plus N channel topics, twelve event types, seven unchanged public routes plus four subscription routes.

## Constitution Check

*GATE: Passed before Phase 0 research and re-checked after Phase 1 design.*

| Gate (Constitution v7.0.0) | Plan evidence | Result |
|---|---|---|
| **I. Independent microservices and domain ownership** | `notification.jar` with its own `NotificationMain`, port 8084, configuration, pool, and schema. Notification owns subscribers, subscriptions, history, selection, delivery records, retries, and idempotency. Transaction only records sales; `TransactionHasNoRecipientLogicTest` forbids subscription types in its sources. `common` gains only Kafka-free framework entries (topic and type registrations, an `Admin` factory in `common/messaging`). No logging service. | PASS |
| **II. Database per service** | `cs6510_notification` with credentials limited to it; no cross-schema access (FR-034, isolation tests). SKU validation uses the catalog *topic*, not Inventory's data (N6). `SubscriptionIndex` is derived only from committed rows, rebuilt at startup, version-checked on every selection, and never updated by a failed change (N5). Reconciliation exports each source with its owner's credentials (N11). | PASS |
| **III. Event-based inter-service communication** | The only new inter-service flow is `SaleCompleted` through Kafka, with a versioned protobuf schema, the 006 envelope, one producer, a documented topic, key, and group, per-key ordering, an idempotent consumer keyed by `sale_id` that acknowledges after commit, and bounded retries then parking ([sale-completed-event.md](contracts/sale-completed-event.md)). Adding the consumer changes no existing consumer. | PASS |
| **IV. External REST API and backward compatibility** | The seven routes, the OpenAPI document, the handlers, and the Load Client are unchanged and hash-checked. Notification's endpoints are additive under `/notifications/`, documented in a versioned contract ([notification-http.md](contracts/notification-http.md), `spec/notification-openapi.yaml`), and overlap no route. | PASS |
| **V. Distributed transactions and Saga coordination** | The Saga is unchanged. T3 stays the retriable post-pivot step and also inserts the outbox row. Repeated completions fail T3's guard and publish nothing. Notification is not a Saga step: Complete never waits on it. | PASS |
| **VI. Checkout and inventory consistency** | Sale existence is tied to durable COMPLETED after the deduction (FR-001). Stock rules are untouched, and 006's invariant checks run after every evidence run. | PASS |
| **VII. Reliable event delivery and analytics accuracy** | `SaleCompleted` goes through Transaction's outbox; every channel publish is backed by a `notification` row and marked only after `acks=all`. Analytics inputs are untouched. Notification outages or backlogs block nothing (FR-033, scenario G). | PASS |
| **VIII. Performance and operational simplicity** | Kafka remains the single broker; channels are Kafka topics (N7). The only new infrastructure is the fourth service, its schema, and the test application. Build, start, ports, configuration, storage, topics, and dependencies are documented ([deployment-and-evidence.md](contracts/deployment-and-evidence.md)). Restarts of each component are tested (scenario F). | PASS |
| **IX. Verification and benchmark evidence** | Tests cover concurrent sales, subscription changes, duplicate delivery, and restarts of Transaction, Notification, the broker, and the subscriber application. The subscriber application has configurable threads, a recorded seed, and structured receipts. Post-drain four-source reconciliation fails on any mismatch, and its detection is itself tested (SC-003). Evidence at four subscriber counts with every required metric, plus the default and stress reports. Updated diagram. | PASS |
| **X. Completed-sale events** | Versioned `SaleCompleted` with a stable sale identity and SKU quantities, inserted in T3 and published only through the outbox. One per sale, enforced by T3's guard and a deterministic event ID under a unique key. No recipient data. | PASS |
| **XI. Subscriptions and notification delivery** | ≥ 100 subscribers with stable identities, one or more SKUs, and a channel topic each. Effective at commit. Selection rule implemented with the clock lock and recorded per sale. One effective notification per (sale, SKU, subscriber) through the unique key and deterministic identity. Redeliveries are distinguishable. Failures are visible, retried, parked, and retryable; nothing is dropped. | PASS |
| **Architectural constraints** | Incremental evolution of 006 at `4a91686` (reuse map below). Endpoint ownership extended without overlap. Per-service reset and seed, with the broker and receipt records in one baseline. The 2,000 × 10,000 baseline is preserved. Concrete endpoints, schemas, and topics live in this plan's contracts, not in the constitution. | PASS |
| **Workflow and quality gates** | Ordering and idempotency keys, retry and parking policies, the selection rule, notification identity and states, retry schedule, and index rebuild are defined in N2–N8 and the contracts. Every Planning Obligation is traced below. Presentation and video are out of scope. | PASS |

**Prerequisite outside this plan**: constitution v7.0.0 is an uncommitted working-tree change with its Sync Impact Report. Governance requires removing the report before committing. The first task of `/speckit-tasks` does that and commits the constitution before or with this feature's first commit.

**Post-design re-check**: [data-model.md](data-model.md), the five contracts, the two [proto baselines](contracts/proto/), and [quickstart.md](quickstart.md) preserve every gate above. No NEEDS CLARIFICATION remains and no deviation exists.

## Planning Obligations Traceability

| Spec obligation | Where it is resolved |
|---|---|
| 1. Completed-sale event | N2, N3; [sale-completed-event.md](contracts/sale-completed-event.md); [proto/sale_events.proto](contracts/proto/sale_events.proto); [data-model.md](data-model.md) §1 |
| 2. Subscription model | N4, N5, N6; [notification-http.md](contracts/notification-http.md); [notification-delivery.md](contracts/notification-delivery.md#subscription-change-transaction); [data-model.md](data-model.md) §2–4 |
| 3. Selection and delivery | N5, N8; [notification-delivery.md](contracts/notification-delivery.md) |
| 4. Subscriber channels | N7; [sale-completed-event.md](contracts/sale-completed-event.md#topics-additions); [proto/notification_events.proto](contracts/proto/notification_events.proto) |
| 5. Subscriber test application | N10; [subscriber-client-and-reconciliation.md](contracts/subscriber-client-and-reconciliation.md); [data-model.md](data-model.md) §5 |
| 6. Reconciliation | N11, N12; [subscriber-client-and-reconciliation.md](contracts/subscriber-client-and-reconciliation.md#reconcilemain); quickstart C, D, E |
| 7. Channel fault injection | N13; [notification-delivery.md](contracts/notification-delivery.md#dispatch); quickstart H |
| 8. Evidence | N15; [deployment-and-evidence.md](contracts/deployment-and-evidence.md#evidence-layout-load-clientreportsnotification-service); quickstart H |

## Phase 0: Research

All decisions, with rationale and alternatives, are in [research.md](research.md):
- N1: baseline and evolution.
- N2: sale identity and `SaleCompleted`.
- N3: the sale topic.
- N4: the subscription model and history.
- N5: selection and the index.
- N6: SKU validation.
- N7: per-subscriber channel topics.
- N8: delivery, retry, charging, and parking.
- N9: delivered vs. received, and latency.
- N10: the subscriber test application.
- N11: reconciliation and drain.
- N12: the change scenario and the causality check.
- N13: fault injection.
- N14: operations and baseline.
- N15: performance and evidence.
- N16: testing.
- N17: deferred options.

## Phase 1: Design and Contracts

| Artifact | Content |
|---|---|
| [data-model.md](data-model.md) | Transaction outbox addition and the T3 change; the Notification schema; state machines; in-memory structures; subscriber files; validation map |
| [contracts/sale-completed-event.md](contracts/sale-completed-event.md) and [contracts/proto/](contracts/proto/) | New topics and event types, envelope values, production, consumption, ordering, compatibility |
| [contracts/notification-http.md](contracts/notification-http.md) | Subscription endpoints with status codes, operational endpoints, gateway change |
| [contracts/notification-delivery.md](contracts/notification-delivery.md) | Change and selection transactions, the dispatcher, the fault and recovery matrix |
| [contracts/subscriber-client-and-reconciliation.md](contracts/subscriber-client-and-reconciliation.md) | `SubscriberMain`, `ChangeScenarioMain`, export, `ReconcileMain` checks, the notification report schema |
| [contracts/deployment-and-evidence.md](contracts/deployment-and-evidence.md) | Units, configuration, baseline, status, harness, evidence layout, diagram |
| [quickstart.md](quickstart.md) | Build, start, scenarios A–H |

### Key flows and statement budgets

These are design budgets, enforced as measured ceilings in `tests/performance/statement-budgets.properties`.

| Flow | Warm-path work | Round trips |
|---|---|---|
| T3 (Transaction consumer) | Guarded UPDATE + attempt UPDATE + **INSERT outbox (`SaleCompleted`)** + commit | 4 (was 3) |
| Subscription change | Clock `FOR UPDATE` + subscriber `FOR UPDATE` + read subscriptions + 3–5 writes + commit | ≤ 9 |
| Selection per sale | Clock `FOR SHARE` + INSERT selection + ⌈n/500⌉ INSERT notifications + commit | 4 for n ≤ 500 |
| Delivery marking, per batch (≤ 200 IDs or 20 ms) | One UPDATE + commit | 2 |
| Failure bookkeeping, per batch | One UPDATE per distinct outcome + commit | ≤ 3 |
| Sweep, every 1 s | One indexed SELECT | 1 |

## Performance Strategy

**Cost model (N15)**:
- The checkout critical path keeps N + 4 commits per basket. T3 gains one INSERT and the dispatcher one send per sale.
- The notification path runs on other threads and other connections: about 1 commit per sale, plus about 1 marking commit per 20 ms.

**Optimizations**, in order:
1. The immediate dispatch path for both `SaleCompleted` and notifications.
2. Shared-lock selection, so selections never block each other.
3. The in-memory index, so selection makes no subscription query on the hot path.
4. Multi-row notification INSERTs.
5. Batched marking.
6. Per-subscriber lanes, so one slow channel cannot hold the shared producer.

**Measurement**: `PERF_METRICS` timers for T3, selection, dispatch queue wait, send-to-confirm, and marking batch size, with counters for charged and uncharged failures, parks, and catch-ups. The harness samples the four backlog figures every 2 s.

**Checkpoints**:
- **CP0**: 006 at `4a91686`, default and stress on this machine.
- **CP1**: `SaleCompleted` in T3, with Notification not running.
- **CP2**: the full system with 100 subscribers.
- **CP3**: optional A/B of consumer threads (4 vs 8), lane in-flight cap (32 vs 64), and marking interval.
- **CP4**: the official runs.

A drop of more than 5% between consecutive checkpoints is investigated before continuing. The interleaved A/B method is that of 006 (`benchmark-compare.ps1`).

## Incremental Evolution Path

Each stage leaves a buildable tree with passing tests. `/speckit-tasks` turns these stages into tasks.

0. **Governance and CP0**: remove the constitution's Sync Impact Report and commit v7.0.0. Run CP0 on `4a91686`.
1. **Contracts and framework**:
   - add `sale_events.proto` and `notification_events.proto` and their baselines;
   - add `TopicNames.SALE_COMPLETED` and the channel name function;
   - add `EventTypes.SALE_COMPLETED` and `SUBSCRIBER_NOTIFIED` with the `notification` producer;
   - add the `KafkaClients.admin` factory;
   - add the sale topic to `create-topics.ps1`;
   - add the schema compatibility tests.
2. **SaleCompleted in Transaction** (CP1):
   - `SaleEvents` (the business builder);
   - the `CheckoutOutcomeHandler` lines source;
   - `CheckoutAttemptStore.complete` accepting the outbox row, and the T3 INSERT with the dispatcher signal;
   - the T3 statement budget raised to 3, with its reason;
   - the `saleEventsUnpublished` status field;
   - `SaleEventOnlyFromT3Test` and `TransactionHasNoRecipientLogicTest`;
   - scenario A.
3. **Notification skeleton**:
   - `db/notification/schema.sql`, `validate.sql`, the bootstrap user, and resets;
   - `NotificationMain`, configuration, pool, HTTP server, and status;
   - `KnownSkus` from catalog replay;
   - subscription endpoints with the change transaction and index;
   - the gateway location;
   - `run-notification.sh` and the `build.sh notification` target.
4. **Selection**: `SaleCompletedConsumer` (messaging), `SaleSelectionHandler` (business), `JdbcSelectionStore`, catch-up, parking of invalid events, and crash-matrix tests on the in-memory bus.
5. **Delivery**: channel provisioning, `NotificationDispatcher` with lanes, marking, the charging rule, parking, the operator endpoints, the test-fault endpoint and channel fault wrapper, and the live Kafka isolation and broker-restart tests.
6. **Subscriber application and reconciler**: `subscriber-client/` (`SubscriberMain`, `ChangeScenarioMain`, `ReconcileMain`), `export-notification-evidence.ps1`, `ReconcileFaultInjectionTest`, and scenarios B–E.
7. **Tooling, documentation, and diagram**:
   - the `reset-baseline.ps1` and port-list extensions;
   - `benchmark-notification.ps1`;
   - the status lines;
   - `server/README.md`, `subscriber-client/README.md`, and `spec/notification-openapi.yaml`;
   - the diagram and its consistency test;
   - the extended architecture tests (CP2 is taken here).
8. **CP3 (optional A/B), then CP4 evidence**: scaling, the default and stress runs, isolation, the change scenario, the README with the comparison, and the quickstart validation record.

### Code reuse map

| Existing code | Plan |
|---|---|
| `CheckoutOutcomeHandler`, `JdbcCheckoutAttemptStore.complete` | Adapted: build and insert `SaleCompleted` in T3; signal the dispatcher after the commit |
| `TransactionMain`'s `OutboxDispatcher`, `JdbcOutboxStore` | Reused unchanged; new topic only |
| `TransactionStatusQueries` | Extended with `saleEventsUnpublished` |
| `common/events` (`ConsumerLoop`, `EnvelopeCodec`, `ParkingPolicy`, `RecordSender`, `OutboxSignal`), `TopicNames`, `EventTypes` | Reused; new registrations only |
| `common/messaging` (`KafkaClients`, `KafkaRecordSender`, `KafkaRecordStream`) | Reused; `KafkaClients.admin` added |
| `common/database` (`ConnectionPool`, `JdbcUnitOfWork`, `JdbcDeadLetterStore`), `common/api`, `common/metrics`, `common/faults` | Reused by Notification |
| Transaction's `CatalogConsumer` pattern (replay from offset 0) | Copied into Notification's `CatalogSkuConsumer` (messaging) with its own handler; no shared business code |
| `db/*.ps1`, `bootstrap.sql`, `reset-baseline.ps1`, `validate-invariants.ps1` | Extended for the fourth schema and port 8084 |
| `deploy/nginx.conf`, `deploy/kafka/*` | Extended: one location, one topic, port lists |
| `server/benchmark-event-based.ps1` | Template for `benchmark-notification.ps1` (shared helper functions moved to a dot-sourced module if duplication exceeds a few functions) |
| `tests/support` (`InMemoryEventBus`, `KafkaTestSupport`, `PublishRawRecord`) | Reused; `InMemoryEventBus` gains dynamic topics for channels |
| Load Client | Unchanged; its Zipf weights are reimplemented in `subscriber-client` for `popularity`, without importing its code |

## Project Structure

### Documentation (this feature)

```text
specs/007-notification-service/
├── spec.md
├── plan.md                         # this file
├── research.md                     # Phase 0
├── data-model.md                   # Phase 1
├── quickstart.md                   # Phase 1
├── contracts/                      # Phase 1
│   ├── sale-completed-event.md
│   ├── notification-http.md
│   ├── notification-delivery.md
│   ├── subscriber-client-and-reconciliation.md
│   ├── deployment-and-evidence.md
│   └── proto/
│       ├── sale_events.proto
│       └── notification_events.proto
├── checklists/requirements.md
└── tasks.md                        # Phase 2, produced by /speckit-tasks
```

### Source code (repository root)

```text
server/
├── contracts/src/main/proto/selfcheckout/events/v1/
│   └── + sale_events.proto, notification_events.proto
├── common/src/
│   ├── events/                     # TopicNames (+ sale topic, channel names), EventTypes (+ 2 types, notification producer)
│   └── messaging/                  # KafkaClients (+ admin)
├── transaction/src/
│   ├── transaction/                # + SaleEvents; CheckoutOutcomeHandler adapted
│   └── database/                   # JdbcCheckoutAttemptStore.complete adapted; TransactionStatusQueries extended
├── notification/src/               # NEW service
│   ├── api/                        # NotificationHttpHandler, OpsHttpHandler, error mapping
│   ├── notification/               # SubscriptionService, SubscriptionIndex, KnownSkus, SaleSelectionHandler,
│   │                               # NotificationDispatcher, ChargingTracker, ChannelFaults, NotificationMain
│   ├── database/                   # JdbcSubscriptionStore, JdbcSelectionStore, JdbcNotificationStore, NotificationStatusQueries
│   └── messaging/                  # SaleCompletedConsumer, CatalogSkuConsumer, ChannelProvisioner (Admin)
├── tests/
│   ├── unit/ database/ architecture/ performance/ messaging/ integration/   # + notification suites
│   └── support/                    # InMemoryEventBus with dynamic topics
├── build.sh                        # + notification target
├── run-notification.sh             # NEW
└── benchmark-notification.ps1      # NEW harness

subscriber-client/                  # NEW, separate test application
├── src/                            # SubscriberMain, SubscriberThread, Assignment, ReceiptWriter, StatusWriter,
│                                   # ChangeScenarioMain, ReconcileMain, Reconciler, NotificationReport
├── test/                           # ReconcileFaultInjectionTest, AssignmentDeterminismTest
├── build.sh  run.sh  README.md

db/
├── notification/                   # NEW schema.sql, validate.sql
├── bootstrap.sql  bootstrap.ps1  reset-service.ps1  reset-baseline.ps1   # extended
└── export-notification-evidence.ps1   # NEW

deploy/
├── nginx.conf                      # + /notifications/
└── kafka/create-topics.ps1         # + sale-completed topic; port lists include 8084

spec/notification-openapi.yaml      # NEW versioned contract for the additive endpoints
docs/architecture/notification-service.{md,svg}   # NEW diagram
load-client/                        # unchanged source
└── reports/notification-service/   # evidence (deployment-and-evidence.md)
```

**Structure Decision**:
- **Fourth ownership root**: `server/notification/` uses the same four layers as the other services. Kafka types stay in `messaging`, and business code sees only `common/events` ports.
- **Subscriber test application outside `server/`**: it sits next to `load-client/`, because it is a client, not a service, and must never share business code with the services. It uses only generated event classes and public interfaces.

## Complexity Tracking

> No constitution violation exists, so the violation table is empty.

**Non-violation complexity notes**:
- **One topic per subscriber**: about 100 or more small topics on a single Windows node, chosen for true channel independence (N7). Index files are capped at 1 MiB per topic to bound memory-mapped files. The baseline removes them all.
- **Charged vs. uncharged failures**: one in-memory timestamp per channel, justified by keeping broker outages from parking notifications while still parking a genuinely faulty channel (N8). Durable state stays authoritative.
- **Global subscription clock lock**: it serializes subscription changes, which are rare, and gives every selection an exact, recordable history point. Selections share the lock with one another.
- **A test-only fault endpoint**: disabled unless `NOTIFICATION_TEST_FAULTS=1`, and needed to start and stop channel faults within one run for SC-006.

## Implementation Notes

*Constitution compliance review at the end of the feature (T130), against constitution v7.0.0. Implementation commits: phases 1–10, `b981692` … `0b33d82`, on `feature/notification-service`.*

**Every gate of the Constitution Check still passes; none changed.** Each row was re-checked against the code and the committed evidence:

| Gate | Re-checked against | Result |
|---|---|---|
| I. Independent microservices | `out/notification.jar` with `notification.NotificationMain`, port 8084, its own pool and schema; `./build.sh notification` builds no other service jar (`IndependentServiceDeploymentTest`); `TransactionHasNoRecipientLogicTest`, `LayerDependencyTest`, `MicroserviceBoundaryTest` | PASS |
| II. Database per service | `DatabaseIsolationTest` (T110): Notification's user is denied every statement on the other three schemas and they on its schema, for disposable and deployed users; its user is DML-only even on its own schema | PASS |
| III. Event-based communication | `SaleCompleted` only through Transaction's outbox and `selfcheckout.sale-completed.v1`; `OutboxOnlyPublishTest`, `EventSchemaCompatibilityTest`; the diagram test finds no synchronous service-to-service edge (T118) | PASS |
| IV. External REST API | `ClientAndContractUnchangedTest` (T106): Load Client and `spec/self-checkout-openapi.yaml` byte-identical to `4a91686`; `nginx.conf` differs only by the Notification upstream and location; `spec/notification-openapi.yaml` is additive | PASS |
| V. Saga coordination | T3 inserts the outbox row in its own commit (statement budget 3 warm, 4 cold, 1 commit); Complete never waits on Notification (`NotificationOutageCheckoutTest`, scenario G) | PASS |
| VI. Consistency | `db/validate-invariants.ps1` passed after every evidence run, the stock invariant included | PASS |
| VII. Reliable delivery, analytics accuracy | Every channel publish is backed by a `notification` row and marked after `acks=all`; `AnalyticsUnaffectedByNotificationsTest` (T105); scenario F | PASS |
| VIII. Performance and operational simplicity | One broker; channels are Kafka topics; the fourth service is the only new infrastructure; documented in `server/README.md` and `deploy/kafka/README.md` | PASS, with the cost below recorded |
| IX. Verification and evidence | Four subscriber counts, both standard reports, isolation, and a reconciliation that is itself tested (SC-003); `load-client/reports/notification-service/README.md` | PASS |
| X. Completed-sale events | One `SaleCompleted` per COMPLETED transaction in every run (reconciler `saleEvents` = 0) and in scenario A | PASS |
| XI. Subscriptions and delivery | Scenarios B, E, F; SC-006 isolation PASS (ratios 1.01 and 0.97) | PASS |
| Architectural constraints | Incremental evolution of `4a91686`; one baseline resets the broker, all four schemas, and the run directory (`ResetBaselineTest`, quickstart §1) | PASS |
| Workflow and quality gates | Tests written before each implementation phase; every phase committed locally with its evidence; coverage ledger "Feature 007" section | PASS |

**Where the implementation refined the plan** (none changes a gate):

- **The Notification user is DML-only** (T012). So its `schema.sql` is applied with the MySQL admin credentials by `db/reset-service.ps1` (and so by `reset-all.ps1` and `reset-baseline.ps1`), and the baseline is then read back as the Notification user. Disposable test users get the same grant (`TestSchema.ownPrivileges()`).
- **Test faults**: channel faults are flags (`notification.channel.reject|stall:<id>`) read by a `ChannelFaults` sender decorator, which only `NotificationMain` constructs. `POST /notifications/ops/faults` exists only with `NOTIFICATION_TEST_FAULTS=1`. The crash points of scenario F use `halt`, which kills the whole JVM, so items 1 and 2 show redeliveries of sends confirmed but not yet marked; every one is kept once by ID.
- **Charging rule consequence**: a failure is charged only while some other channel confirms sends. After the load ends, a faulted channel's remaining failures are broker-wide, so they are uncharged and never park. The harness's faulted runs therefore drain twice: first the other subscribers, then everyone, after it clears the fault and runs the operator retry. No notification was parked in the isolation runs; all were delivered after the channel recovered.
- **Healthy isolation runs record their fault subscriber** too, so `endToEndExcluding` measures the same 99 subscribers in all three modes.
- **Exports** compute epoch times with `TIMESTAMPDIFF` from the stored values. The services write `TIMESTAMP(3)` with `serverTimezone=UTC` into a server whose session zone is UTC−8, which makes `UNIX_TIMESTAMP()` and `NOW()` comparisons wrong. The services are self-consistent, and every comparison is done in one form.
- **Diagram**: feature 006's diagram stays as it was, checked against 006's topics only. Feature 007's flow is its own page, `docs/architecture/notification-service.md`, with its own consistency checks (T118).
- **Harness additions**:
  - `-Disruption` kinds for scenario F;
  - `notification-stopped` for scenario G;
  - `-Fault` for scenario H;
  - `run-evidence.ps1` for lists of runs;
  - port 8084 added to the 006 harnesses' checks (T114);
  - `server/build.sh all` accepted as the quickstart writes it.
- **A channel is ready only once it can take writes** (found by the final gate, T129). On the Windows broker a new topic's leader is visible before its partition accepts writes, and the first sends can fail with `NotLeaderOrFollowerException` for many seconds. `KafkaChannelProvisioner.provision` therefore waits, bounded at 30 s, until the new partition answers a ListOffsets request. If it doesn't, the channel stays not ready and provisioning is retried as before. The benchmarks never showed this, because their channels are created at registration, long before the first sale.
- **Tests made robust**:
  - `ResetBaselineTest` reads the reset script's output from a file, because the broker restarted by the reset inherits the pipe.
  - `NotificationOutageCheckoutTest` counts commits outside the outbox marker, whose batching depends on timing.
  - `KafkaTestSupport` waits for the same partition readiness after creating test topics. That ended the long-known flaky failure of `KafkaTransportLiveTest`'s ordering test.
  - `SaleEventParkingLiveTest` named its two same-partition sales after the partition only. When both bad records landed on one partition, about 1 run in 16, it built one sale twice and waited for a tenth delivery that could not come.

**Performance (Gate VIII, recorded, not a deviation)**:
- **The cost of a fourth service**: the plan estimated at most 3% for the `SaleCompleted` row (research N15), and CP1 measured +0.1%. Running Notification itself on the same host and MySQL server costs about 7% of default throughput and about 30 ms of Complete p95. This was measured against same-day controls without Notification (CP2, `load-client/reports/notification-service/checkpoints.md`). The cost does not grow with the subscriber count.
- **Correctness is unaffected**: every run reconciles, and checkout keeps no dependency on Notification.
- **CP3 was not run**: it was the optional tuning of consumer threads, lane cap, and marking interval. `benchmark-compare.ps1` drives only the 006 harness. The marking interval is the setting most worth trying first.

**Out of scope, as planned**: presentation and video; tuning beyond the defaults (CP3).
