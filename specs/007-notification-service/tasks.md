---

description: "Task list for adding the Notification Service, the subscriber test application, and independent reconciliation"
---

# Tasks: Sale Notification Service with Subscriber Reconciliation

**Input**: Design documents from `/specs/007-notification-service/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md) (N1–N17), [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)

**Tests**: Requested by spec FR-038, Principle IX, and plan N16. Within each story, write the test tasks first and confirm they fail before starting the implementation tasks. Feature 006's existing tests must keep passing with unweakened assertions (FR-031).

**Organization**: Tasks are grouped by user story, in the spec's priority order: US1 → US2 → US3 (P1), US4 → US5 → US6 → US7 (P2), US8 (P3). All stories are required; priority sets delivery order only.
- **Checkpoints**: CP0 (T002), CP1 (end of US1, T035), CP2 (T122), CP3 (T123, optional), CP4 (T124–T126). See plan.md *Performance Strategy*.
- **Task IDs** are in execution order. Execute tasks in document order within a phase, subject to the dependencies below.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel: different files, and no dependency on an incomplete task.
- **[Story]**: The user story the task serves (US1–US8).
- Every task names its exact file paths.

## Path Conventions

- **Production code**: `server/<service>/src/<package>/…`, where the package is the directory name (`package notification;`). Notification's packages are `api`, `notification` (business), `database`, and `messaging`. Shared code lives in `server/common/src/{events,messaging,database,api,metrics,faults,time}/`. Generated event classes live in `server/contracts/` (`package selfcheckout.events.v1`).
- **Server tests**: `server/tests/{unit,architecture,database,contract,integration,messaging,performance,support}/…`. They are compiled by `server/build-tests.sh` and run by `server/run-tests.sh [package …]`.
- **Subscriber test application**: `subscriber-client/src/*.java` (default package, like `load-client/`). Its tests are in `subscriber-client/test/*.java`, run by `subscriber-client/run-tests.sh`.
- **Database and deployment**: `db/notification/`, `db/*.ps1`, `deploy/nginx.conf`, `deploy/kafka/`.
- **Evidence**: `load-client/reports/notification-service/`, `docs/architecture/notification-service.{md,svg}`.

## Implementation Rules

These rules bind every task below.

1. **Carried over from feature 006** (`specs/006-event-based-architecture/tasks.md`, *Implementation Rules* 1–7). Process-local structures are never authoritative. An offset is acknowledged only after commit. Kafka types appear only in `server/common/src/messaging/` and `server/<service>/src/messaging/`, wired only by `*Main` composition roots. The shared JDBC adapters for `outbox_event` and `dead_letter_event` are used as they are. Benchmark claims rest only on genuine runs.
2. **No publish without a durable row** (Principle VII): only `OutboxDispatcher`, `KafkaScanEventSink`, and `NotificationDispatcher` may call a `RecordSender`. `NotificationDispatcher` sends only rows that already exist in `notification`, with their stored identity and contents.
3. **No recipient logic in Transaction** (FR-004): no class under `server/transaction/src/` may reference subscribers, subscriptions, recipients, or the `cs6510_notification` schema.
4. **Subscriptions effective only at commit** (FR-007, FR-008): `SubscriptionIndex` changes only after a committed change, or from committed `subscription_change` rows during catch-up. It never changes from request data.
5. **Shared machine**: never build or run tests while a benchmark runs. Run `server/benchmark-compare.ps1` and `server/benchmark-notification.ps1` with `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`. Retry a service start that fails with a `BindException`. Save scratch `.ps1` files that contain the repository path as UTF-8 with BOM. Docker is never used.
6. **Credentials**: never print database passwords or pass them as arguments. Use the per-service `*_DB_PASSWORD` user environment variables, including the new `NOTIFICATION_DB_PASSWORD`, which the user sets, and `DB_USER`/`DB_PASSWORD` for admin work.
7. **Broker**: the native Kafka 4.1.0 broker under `KAFKA_HOME`. Topics are never deleted individually. Only `deploy/kafka/reset-topics.ps1` clears them, by replacing the log directory.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Commit the governing constitution, record the baseline, and add the new schemas, topics, and directories.

- [X] T001 Commit constitution v7.0.0 in `.specify/memory/constitution.md`:
  - Show the user the working-tree diff of the principles and governance text (excluding the Sync Impact Report), and get explicit approval of that exact text before committing.
  - Remove the leading `<!-- Sync Impact Report … -->` comment block, as Governance requires, leaving every principle unchanged.
  - Commit it alone, or together with `specs/007-notification-service/`, before any code change of this feature.
- [X] T002 Run CP0 on the unmodified feature 006 implementation at commit `4a91686`, before any code change:
  - Build with `server/build.sh` and `load-client/build.sh`.
  - Run `server/benchmark-event-based.ps1` with the shared-machine settings: twice with `-Workload default` (labels `cp0-default-1`, `cp0-default-2`) and once with `-Workload stress` (label `cp0-stress-1`), writing output under `load-client/reports/notification-service/cp0/`.
  - Record `totalTransactions`, per-operation p95/p99, and the invariant results in a new `load-client/reports/notification-service/checkpoints.md`.
- [X] T003 [P] Add `server/contracts/src/main/proto/selfcheckout/events/v1/sale_events.proto` and `notification_events.proto`, copied field-for-field from `specs/007-notification-service/contracts/proto/`. Run `server/build.sh contracts` and confirm that `SaleCompleted`, `SoldLine`, and `SubscriberNotified` are generated in `out/contracts.jar`.
- [X] T004 [P] Copy the two `.proto` files into `server/tests/messaging/baseline/` as their version-1 baselines, next to the 006 baselines.
- [X] T005 [P] Add the topic `selfcheckout.sale-completed.v1` to `deploy/kafka/create-topics.ps1`: 16 partitions, `cleanup.policy=delete`, and the 006 Windows pins `segment.bytes=1073741824` and `segment.ms=9223372036854775807`. Verify it alongside the four 006 topics, and extend the default `-Ports` of `Test-ServicePortAnswers` in `deploy/kafka/kafka-tools.ps1`, and the port checks in `deploy/kafka/reset-topics.ps1`, from `8081, 8082, 8083` to `8081, 8082, 8083, 8084`.
- [X] T006 [P] Create the empty directories `server/notification/src/{api,notification,database,messaging}/`, `subscriber-client/{src,test}/`, `db/notification/`, and `load-client/reports/notification-service/`, each with a `.gitkeep` where it would otherwise be empty.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Register the new topics and types, create the Notification schema and user, and give Notification Service a buildable, startable skeleton. Every story depends on this phase.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T007 Extend `server/common/src/events/TopicNames.java`:
  - add `Topic.SALE_COMPLETED("selfcheckout.sale-completed.v1", 16)`;
  - add `NOTIFICATION_SALE_COMPLETED_GROUP = "notification-sale-completed"`;
  - add `saleCompleted()`;
  - add `channel(String subscriberId)`, returning `prefix + "selfcheckout.notify." + subscriberId + ".v1"`, which rejects IDs that do not match `^[A-Za-z0-9_-]{1,40}$`;
  - add `isChannel(String name)`.

  `all()` returns the five fixed topics in catalog order (catalog, checkout, checkout-outcome, scan-accepted, sale-completed).
- [X] T008 Extend `server/common/src/events/EventTypes.java`:
  - add the producer constant `NOTIFICATION = "notification"`;
  - add `SALE_COMPLETED("SaleCompleted", Topic.SALE_COMPLETED, TRANSACTION, SaleCompleted.parser(), p -> ((SaleCompleted) p).getTransactionId())`;
  - add `SUBSCRIBER_NOTIFIED` with producer `NOTIFICATION` and routing key `subscriber_id`; its topic is the subscriber's channel, so model it as a channel-family type whose `topic()` is empty and add `isChannelType()`.

  Add `server/common/src/events/EventIds.java` with `saleEventId(String transactionId)`, returning `UUID.nameUUIDFromBytes(("sale:" + transactionId).getBytes(UTF_8)).toString()`, and `notificationId(String saleId, String sku, String subscriberId)` over `"notification:" + saleId + ":" + sku + ":" + subscriberId`. Keep `EnvelopeCodec` validation working for channel-family types (producer and routing-key checks).
- [X] T009 [P] Add `KafkaClients.admin(String bootstrap)` to `server/common/src/messaging/KafkaClients.java`, returning a configured `org.apache.kafka.clients.admin.Admin` with `request.timeout.ms=5000` and `default.api.timeout.ms=10000`.
- [X] T010 Write `db/notification/schema.sql` exactly per [data-model.md](data-model.md) §2, with InnoDB tables and `utf8mb4`:
  - `subscription_clock` (`id` TINYINT PK with CHECK `id = 1`; `version` BIGINT NOT NULL), seeded with `(1, 0)`.
  - `subscriber`: `subscriber_id` VARCHAR(40) PK; `status` ENUM('ACTIVE','CANCELLED'); `channel_topic` VARCHAR(200) NOT NULL; `channel_ready` BOOLEAN NOT NULL DEFAULT FALSE; `created_version` and `updated_version` BIGINT NOT NULL; `created_at` and `updated_at` TIMESTAMP(3).
  - `subscription`: `subscriber_id` VARCHAR(40) FK → `subscriber`; `sku` VARCHAR(20); `added_version` BIGINT NOT NULL; PK (`subscriber_id`, `sku`); index (`sku`, `subscriber_id`).
  - `subscription_change`: `version` BIGINT PK; `subscriber_id` VARCHAR(40) NOT NULL; `change_type` ENUM('REGISTER','UPDATE','CANCEL'); `added_skus`, `removed_skus`, and `resulting_skus` TEXT NOT NULL; `committed_at` TIMESTAMP(3) NOT NULL.
  - `sale_selection`: `sale_id` VARCHAR(40) PK; `sale_event_id` CHAR(36) NOT NULL; `completed_at` TIMESTAMP(3) NOT NULL; `lines` TEXT NOT NULL; `subscription_version` BIGINT NOT NULL; `recipient_count` and `notification_count` INT NOT NULL; `selected_at` TIMESTAMP(3) NOT NULL.
  - `notification`: `notification_id` CHAR(36) PK; `sale_id` VARCHAR(40) NOT NULL FK → `sale_selection`; `sku` VARCHAR(20) NOT NULL; `subscriber_id` VARCHAR(40) NOT NULL; `quantity` INT NOT NULL with CHECK > 0; `completed_at` TIMESTAMP(3) NOT NULL; `state` ENUM('PENDING','DELIVERED','PARKED'); `attempt_count`, `charged_failures`, `uncharged_failures`, and `operator_retries` INT NOT NULL DEFAULT 0; `next_attempt_at` TIMESTAMP(3) NOT NULL; `last_error` VARCHAR(255) NULL; `created_at` TIMESTAMP(3) NOT NULL; `delivered_at` and `parked_at` TIMESTAMP(3) NULL; UNIQUE (`sale_id`, `sku`, `subscriber_id`); INDEX (`state`, `next_attempt_at`); INDEX (`subscriber_id`, `state`); CHECKs `(state = 'DELIVERED') = (delivered_at IS NOT NULL)` and `(state = 'PARKED') = (parked_at IS NOT NULL)`.
  - `dead_letter_event`: identical to `db/transaction/schema.sql`'s.
- [X] T011 [P] Write `db/notification/validate.sql`, covering:
  - `subscription_change.version` is contiguous from 1 to `subscription_clock.version`;
  - every `notification.sale_id` has a `sale_selection`;
  - `sale_selection.notification_count` equals its rows;
  - no `DELIVERED` row lacks `delivered_at`;
  - counts by state, and parked events.
- [X] T012 Extend `db/bootstrap.sql` and `db/bootstrap.ps1`:
  - `CREATE DATABASE IF NOT EXISTS cs6510_notification`;
  - user `cs6510_notification_svc` at `localhost` and `127.0.0.1`, identified by the `{{NOTIFICATION_DB_PASSWORD}}` placeholder, with `REVOKE ALL` and then `SELECT, INSERT, UPDATE, DELETE` on `cs6510_notification.*` only, as for the other three users;
  - `bootstrap.ps1` reads `NOTIFICATION_DB_PASSWORD` from the user environment without printing it, and fails with a clear message if it is unset.
- [X] T013 Extend `db/reset-service.ps1` (new `-Service notification`, which applies `db/notification/schema.sql` with the admin credentials), `db/reset-all.ps1` (four services), and `db/reset-baseline.ps1`:
  - refuse to run while 8081–8084 answer;
  - reset Notification to `subscription_clock.version = 0` and no other rows;
  - add `-RunDir`, which removes that run directory after the broker reset;
  - print the Notification counts and the five-topic list in the baseline summary.
- [X] T014 Add the `notification` target to `server/build.sh`, building `out/notification.jar` from `server/notification/src` against `out/common.jar`, `out/contracts.jar`, `lib/messaging/*`, and Connector/J; include it in `all`. Add `out/notification.jar` to the classpaths in `server/build-tests.sh` and `server/run-tests.sh`. Create `server/run-notification.sh`, mirroring `server/run-transaction.sh`, with main class `notification.NotificationMain`.
- [X] T015 Create `server/notification/src/notification/NotificationConfig.java`, reading every variable in the configuration table of [contracts/deployment-and-evidence.md](contracts/deployment-and-evidence.md#notification-configuration) with its defaults (`NOTIFICATION_HTTP_PORT` 8084, `NOTIFICATION_DB_POOL_SIZE` 12, `NOTIFICATION_DB_POOL_MAX_SIZE` 20, `NOTIFICATION_SALE_CONSUMER_THREADS` 4, `NOTIFICATION_LANE_IN_FLIGHT` 64, `NOTIFICATION_PARK_AFTER_CHARGED` 8, `NOTIFICATION_SWEEP_MS` 1000, `NOTIFICATION_TEST_FAULTS` unset). Create `server/notification/src/notification/NotificationMain.java` as the composition root:
  - the `ConnectionPool` for `cs6510_notification`, the JDK `HttpServer` on the port, and `StatusReporter`;
  - shutdown in the 006 order: HTTP, then consumers, dispatcher, workers, producers, and the pool;
  - it starts with the broker down, because producers are lazy.
- [X] T016 [P] Add to `deploy/nginx.conf` the upstream `notification_service { server 127.0.0.1:8084; }` and `location ^~ /notifications/ { proxy_pass http://notification_service; }` with the same proxy settings and `@upstream_unavailable` handling as the existing locations. Leave every existing location byte-identical.
- [X] T017 [P] Add `server/tests/support/NotificationTestSchema.java` (apply and reset `db/notification/schema.sql` in a test schema) and `server/tests/support/InProcessNotification.java` (start Notification's components against a schema and an `InMemoryEventBus`, with `restart()` and `crash()`). Extend `server/tests/support/InMemoryEventBus.java` so topics are created on first use, which channel topics need, and so consumers can be manually assigned per topic.
- [X] T018 Extend the architecture tests to four services:
  - `server/tests/architecture/LayerDependencyTest.java`: Notification's `api` → business → `database`, with Kafka only in `messaging`;
  - `MicroserviceBoundaryTest.java`: no imports between `transaction`, `inventory`, `analytics`, and `notification`;
  - `CompositionRootTest.java`: `NotificationMain`'s shutdown order;
  - `OutboxOnlyPublishTest.java`: `NotificationDispatcher` is the only Notification caller of `RecordSender` (Implementation Rule 2).
- [X] T019 Extend `server/tests/messaging/EventSchemaCompatibilityTest.java` to the two new baselines (no renumbered or retyped field, and the parsers round-trip), and add `SALE_COMPLETED` and `SUBSCRIBER_NOTIFIED` to its producer and routing-key assertions.

**Checkpoint**: `server/build.sh all` and `server/run-tests.sh architecture messaging` pass. `server/run-notification.sh` starts, `GET :8084/notifications/ops/status` is not yet implemented (404 is fine), and `db/reset-baseline.ps1` resets four schemas and five topics.

---

## Phase 3: User Story 1 - Record Every Completed Sale Exactly Once (Priority: P1) 🎯 MVP

**Goal**: T3 records exactly one versioned `SaleCompleted` with the sale identity and SKU quantities, and publishes it through Transaction's outbox (FR-001–FR-005).

**Independent Test**: With Notification stopped, complete known baskets, rejections, repeats, and background completions. The `SaleCompleted` outbox rows and topic records match the COMPLETED transactions exactly (quickstart scenario A, SC-001).

### Tests for User Story 1 ⚠️

- [X] T020 [P] [US1] Write `server/tests/unit/transaction/SaleEventsTest.java`:
  - building from lines {B:1, A:3} gives `lines` [A×3, B×1] in ascending, unique order;
  - `sale_id = transaction_id`;
  - `event_id = EventIds.saleEventId(transactionId)`;
  - envelope `routing_key` and `correlation_id` are the transaction ID and `producer` is `transaction`;
  - a zero quantity, a duplicate SKU, or an empty basket throws.
- [X] T021 [P] [US1] Extend `server/tests/database/JdbcCheckoutAttemptStoreTest.java`:
  - `complete` inserts exactly one `outbox_event` row with `event_type = 'SaleCompleted'`, `aggregate_key = 'sale:<id>'`, and the deterministic `event_id`, in the same commit as `status = 'COMPLETED'`;
  - a guard miss inserts nothing;
  - a forced failure of the outbox INSERT rolls back the completion, so the transaction keeps its pending attempt;
  - a second insert of the same `event_id` violates `uq_outbox_event_id`.
- [X] T022 [P] [US1] Write `server/tests/integration/SaleCompletedEventTest.java` on `InProcessCheckout` with `InMemoryEventBus`, covering spec US1 acceptance scenarios 1–5 and 7:
  - 3×A + 1×B gives one event with A = 3 and B = 1;
  - a rejection followed by a successful second attempt gives one event, for attempt 2;
  - a repeated Complete, a redelivered `StockDeducted`, and a Transaction restart give no additional event;
  - scans, pending attempts, and rejected attempts give no event;
  - a background completion after `500` gives one event;
  - the payload has no recipient field.
- [X] T023 [P] [US1] Write `server/tests/messaging/SaleEventPublishLiveTest.java` (live Kafka, per-run prefix), covering US1 scenario 6: with the broker stopped through `KafkaTestSupport`, a completion commits and Complete answers 200; after the broker restarts, the `SaleCompleted` record appears exactly once on `selfcheckout.sale-completed.v1`, keyed by the transaction ID.
- [X] T024 [P] [US1] Write `server/tests/architecture/TransactionHasNoRecipientLogicTest.java`, which scans `server/transaction/src/**` for `subscri`, `recipient`, `cs6510_notification`, and imports of `notification.*` (FR-004). Write `server/tests/architecture/SaleEventOnlyFromT3Test.java`: the only references to `SaleEvents`/`SALE_COMPLETED` in Transaction are in `CheckoutOutcomeHandler`, `JdbcCheckoutAttemptStore`, `SaleEvents`, and `TransactionMain`.
- [X] T025 [P] [US1] Write `server/tests/messaging/SaleEventContractTest.java`: a `SaleCompleted` produced by the Transaction code passes `EnvelopeCodec` validation as a Notification consumer would apply it, and records with a wrong producer, a wrong routing key, an unsupported version, unsorted lines, or a non-positive quantity are rejected as permanent.
- [X] T026 [US1] Update `server/tests/performance/statement-budgets.properties` to `complete.t3.statements=3` and `complete.t3.statements.design=3` (commits stay 1), with a comment citing FR-002 and research N2. Extend `server/tests/performance/OutcomeHandlerStatementBudgetTest.java` so T3 is measured at exactly 3 statements and 1 commit on the warm path, and at 4 statements on a cache miss (the extra read is `CheckoutAttemptStore.find`, before T3).

### Implementation for User Story 1

- [X] T027 [US1] Create `server/transaction/src/transaction/SaleEvents.java`. It builds the `SaleCompleted` `OutboxRow` from `(transactionId, attemptNumber, completedAt, stationId, lines)` using `EventIds.saleEventId`, `EnvelopeCodec`, and `TopicNames.saleCompleted()`; it validates lines as in T020; and it sets `aggregateKey = "sale:" + transactionId`.
- [X] T028 [US1] Change `server/transaction/src/database/CheckoutAttemptStore.java` and `JdbcCheckoutAttemptStore.java` so that `complete(AttemptKey key, Instant completedAt, boolean answering, OutboxRow saleCompleted)` runs, in one `JdbcUnitOfWork`, `COMPLETE_GUARD`, then `COMPLETE_ATTEMPT`, then `JdbcOutboxStore.insert(connection, saleCompleted)`, then `FaultHooks.hit("transaction.sale.beforeT3Commit")`, then commit. A guard miss still rolls back with `GUARD_MISS` and inserts nothing.
- [X] T029 [US1] Adapt `server/transaction/src/transaction/CheckoutOutcomeHandler.java`:
  - in `complete(AttemptKey)`, take the attempt's lines from the cached `TransactionStateCache.State` when present, otherwise from `attempts.find(key)`'s canonical `lines`;
  - check that the line quantities sum to the attempt's `item_count`, and raise an integrity fault (existing `integrity(...)` path) on a mismatch;
  - build the row with `SaleEvents`, and call the new `complete`;
  - after a successful commit, call `FaultHooks.hit("transaction.sale.afterT3Commit")` and signal the existing `OutboxSignal` with the event ID, before completing the settlement future.
- [X] T030 [US1] Wire `SaleEvents` (with `TopicNames.fromEnvironment`) into `server/transaction/src/transaction/TransactionMain.java`, passing it to `CheckoutOutcomeHandler`. The existing `OutboxDispatcher` publishes the new rows unchanged.
- [X] T031 [US1] Add `saleEventsUnpublished` (`COUNT(*) FROM outbox_event WHERE event_type = 'SaleCompleted' AND published_at IS NULL`) and `saleEventsTotal` to `server/transaction/src/database/TransactionStatusQueries.java`, show them in the Transaction status line, and extend `server/tests/database/ServiceStatusQueriesTest.java` to cover them.
- [X] T032 [US1] Add the SC-001 invariant to `db/transaction/validate.sql`: the number of COMPLETED transactions equals the number of `SaleCompleted` outbox rows, and every COMPLETED transaction has exactly one row with `aggregate_key = CONCAT('sale:', transaction_id)`. Make `db/validate-invariants.ps1` report the check and fail on it.
- [X] T033 [US1] Run `server/run-tests.sh unit database integration messaging architecture performance` and confirm that T020–T026 pass and every 006 test still passes.
- [X] T034 [US1] Execute quickstart scenario A by hand with the Notification process absent, and record the commands and results in `load-client/reports/notification-service/us1-scenario-a.md`.
- [X] T035 [US1] Run CP1: `server/benchmark-event-based.ps1` with this tree, default twice (`cp1-default-1`, `cp1-default-2`) and stress once (`cp1-stress-1`), under `load-client/reports/notification-service/cp1/`. Add the comparison with CP0 to `checkpoints.md`, and investigate and explain any drop of more than 5% before continuing (the design estimate is ≤ 3%, research N15).

**Checkpoint**: completed sales are recorded and published exactly once; checkout behavior is otherwise unchanged.

---

## Phase 4: User Story 2 - Notify Each Subscriber Once per Sold SKU (Priority: P1)

**Goal**: Notification Service registers subscribers, selects recipients per sale under the Recipient Selection Rule, and delivers one `SubscriberNotified` per (sale, SKU, subscriber) through each subscriber's own channel (FR-006 registration, FR-008–FR-016).

**Independent Test**: Register S1 = {A, B} and S2 = {B}, then complete 3×A + 1×B + 2×C. S1's channel holds A×3 and B×1, S2's holds B×1, and nobody is notified for C. A C-only sale records a selection with zero recipients. With S1's channel faulted, S2 still receives its notifications.

### Tests for User Story 2 ⚠️

- [X] T036 [P] [US2] Write `server/tests/unit/notification/SubscriptionIndexTest.java`:
  - snapshots are immutable, with SKUs mapped to sorted subscriber IDs;
  - `apply(change)` requires `change.version == index.version + 1`, and a gap is reported for catch-up;
  - `resulting_skus` replaces the subscriber's set, and `CANCEL` removes it;
  - a rebuild from rows plus the clock version is equal to incremental application.
- [X] T037 [P] [US2] Write `server/tests/unit/notification/SelectionPlanTest.java`: lines × index snapshot give exactly one planned notification per (SKU, subscriber) for subscribed SKUs only, with the line's quantity and `EventIds.notificationId`; a sale with no subscribed SKU gives zero notifications and a selection with `recipient_count = 0`; `recipient_count` counts distinct subscribers.
- [X] T038 [P] [US2] Write `server/tests/unit/notification/KnownSkusTest.java`: the set is empty and `loaded() == false` until a `CatalogSnapshotPublished` whose `product_count` matches the received items of its version; a newer version replaces the set; stale items are ignored.
- [X] T039 [P] [US2] Write `server/tests/database/JdbcSubscriptionStoreTest.java` for registration:
  - the clock is locked `FOR UPDATE`;
  - a new subscriber gives `version + 1`, one `subscription_change` (`REGISTER`), and `subscription` rows;
  - an identical repeat gives no new row and returns `updated_version`;
  - a different SKU set for an active subscriber gives `SUBSCRIBER_EXISTS` and no change;
  - `FaultHooks` `notification.subscription.beforeCommit:throw` leaves no row and the clock unchanged.
- [X] T040 [P] [US2] Write `server/tests/database/JdbcSelectionStoreTest.java`:
  - the selection reads the clock `FOR SHARE` and records that version;
  - a duplicate `sale_id` returns `ALREADY_SELECTED` and inserts nothing;
  - 1,201 notifications are inserted in three statements (chunks of ≤ 500);
  - a zero-recipient sale inserts only `sale_selection`;
  - the catch-up query returns `subscription_change` rows in version order.
- [X] T041 [P] [US2] Write `server/tests/database/JdbcNotificationStoreTest.java`:
  - `markDelivered(ids, at)` updates only `PENDING` rows and sets `delivered_at`;
  - `recordFailures` updates `attempt_count`, the charged or uncharged count, `last_error`, and `next_attempt_at`;
  - `park` sets `PARKED` and `parked_at`;
  - `selectDue(now, 1000)` orders by `next_attempt_at` and excludes `DELIVERED` and `PARKED`;
  - both CHECK constraints reject inconsistent rows.
- [X] T042 [P] [US2] Write `server/tests/unit/notification/NotificationDispatcherTest.java` with `CountingRecordSender` and a manual clock:
  - the immediate path sends after intake;
  - lanes hold at most 64 in flight per subscriber and are served round-robin;
  - confirmations are batched (20 ms or 200 IDs);
  - failures schedule `min(200 ms × 2^n, 5 s)`;
  - the sweep re-sends due rows after a simulated restart with the same `notification_id`;
  - a channel that is not ready is provisioned before its first send.
- [X] T043 [P] [US2] Write `server/tests/contract/NotificationSubscribersEndpointTest.java` for `POST /notifications/subscribers` and `GET /notifications/subscribers/{id}`, with every row of [contracts/notification-http.md](contracts/notification-http.md): 201, 200 for a repeat, 409 `SUBSCRIBER_EXISTS`, 400 `INVALID_SUBSCRIBER_ID`, 400 `MISSING_SKUS`, 400 `UNKNOWN_SKU`, 400 `INVALID_REQUEST`, 500 while the catalog is not loaded, 404 `SUBSCRIBER_NOT_FOUND`, the validation precedence, and the subscriber view fields.
- [X] T044 [P] [US2] Write `server/tests/integration/NotificationFlowTest.java` on `InProcessCheckout` + `InProcessNotification` + `InMemoryEventBus`, covering spec US2 acceptance scenarios 1–4 and 6: per-SKU notifications and quantities, a zero-recipient selection, the selection recorded before acknowledgement, two sales of one SKU, and a stable identity across a forced re-send.
- [X] T045 [P] [US2] Write `server/tests/messaging/ChannelIsolationLiveTest.java` (live Kafka), covering US2 scenario 5:
  - register 3 subscribers;
  - set `notification.channel.reject:<s1>` and then `notification.channel.stall:<s1>` through `ChannelFaults`;
  - complete sales: s2 and s3 receive every notification on their own topics with no added delay beyond 1 s, while s1's rows stay `PENDING` with charged failures;
  - after the fault is cleared, s1's notifications are delivered.
- [X] T046 [P] [US2] Write `server/tests/performance/SelectionStatementBudgetTest.java` and add `notification.select.statements=3` (for n ≤ 500, plus 1 per further 500) and `notification.select.commits=1` to `statement-budgets.properties`.

### Implementation for User Story 2

- [X] T047 [US2] Create `server/notification/src/notification/KnownSkus.java` (the business handler for `CatalogItemPublished` and `CatalogSnapshotPublished`, holding the complete SKU set per version) and `server/notification/src/messaging/CatalogSkuConsumer.java`, a manually assigned consumer of `TopicNames.catalog()` that replays from offset 0 at every start, modeled on `server/transaction/src/messaging/CatalogConsumer.java` and parking invalid records in Notification's `dead_letter_event`.
- [X] T048 [US2] Create `server/notification/src/notification/SubscriptionIndex.java`: an immutable snapshot `{version, Map<String sku, String[] sortedSubscriberIds>}` behind an `AtomicReference`, with `apply(List<ChangeRow>)` in strict version order, `rebuild(rows, version)`, and `lookup(sku)`.
- [X] T049 [US2] Create the `server/notification/src/database/SubscriptionStore.java` port and `JdbcSubscriptionStore.java`, implementing the change transaction of [contracts/notification-delivery.md](contracts/notification-delivery.md#subscription-change-transaction) for `REGISTER`, including re-registration of a `CANCELLED` subscriber, together with `find(subscriberId)`, `loadIndex()` (subscriptions plus clock in one consistent read), and `changesAfter(version, upTo)`. Store canonical sorted JSON arrays in `added_skus`, `removed_skus`, and `resulting_skus`.
- [X] T050 [US2] Create `server/notification/src/notification/SubscriptionService.java`:
  - validation in the precedence of the HTTP contract, with the subscriber ID matching `^[A-Za-z0-9_-]{1,40}$` and SKUs checked against `KnownSkus`;
  - `register`, which calls the store and, only after commit, applies the change to `SubscriptionIndex` (catching up from `changesAfter` on a gap) and provisions the channel;
  - an ambiguous commit returns `INTERNAL_ERROR` and leaves the index untouched.
- [X] T051 [US2] Create the `server/notification/src/notification/ChannelProvisioner.java` port and `server/notification/src/messaging/KafkaChannelProvisioner.java`, which uses `KafkaClients.admin` to create `TopicNames.channel(id)` with 1 partition, replication 1, `cleanup.policy=delete`, `segment.bytes=1073741824`, `segment.ms=9223372036854775807`, `segment.index.bytes=1048576`, and no retention, treating `TopicExistsException` as success. On success, set `subscriber.channel_ready = TRUE` through `JdbcSubscriptionStore.markChannelReady`.
- [X] T052 [US2] Create `server/notification/src/api/NotificationHttpHandler.java` and `NotificationErrorMapper.java` for `POST /notifications/subscribers` and `GET /notifications/subscribers/{id}`, using `common/api` `HttpSupport`, `Json`, and `ApiErrors`, with the subscriber view of the contract. Messages never name Kafka, offsets, or SQL.
- [X] T053 [US2] Create the `server/notification/src/database/SelectionStore.java` port and `JdbcSelectionStore.java`, implementing the selection transaction of [contracts/notification-delivery.md](contracts/notification-delivery.md#selection-transaction): clock `FOR SHARE`, a catch-up callback, the `sale_selection` INSERT with duplicate detection, notification INSERTs in chunks of ≤ 500, and the `notification.selection.beforeCommit` and `afterCommit` hooks.
- [X] T054 [US2] Create `server/notification/src/notification/SaleSelectionHandler.java`, an `EventHandler` for `SaleCompleted`:
  - validate the payload (lines non-empty, unique, ascending; quantities ≥ 1; `sale_id = transaction_id = routing_key`), throwing `PermanentEventFailure` otherwise;
  - plan the notifications against the index after catch-up to the locked version, and run the selection;
  - treat a duplicate as a no-op;
  - hand new rows to the dispatcher after the commit;
  - map `StoreFailure` to `TransientEventFailure`.
- [X] T055 [US2] Create `server/notification/src/messaging/SaleCompletedConsumer.java`, wiring `ConsumerLoop` on `TopicNames.saleCompleted()`, group `NOTIFICATION_SALE_COMPLETED_GROUP`, with `NOTIFICATION_SALE_CONSUMER_THREADS` threads, `fetch.max.wait.ms=50`, and parking in Notification's `dead_letter_event` through `JdbcDeadLetterStore`.
- [X] T056 [US2] Create the `server/notification/src/database/NotificationStore.java` port and `JdbcNotificationStore.java`, with `markDelivered`, `recordFailures`, `park`, `selectDue`, `retryParked(Optional<String> subscriberId)`, and the counts by state, as tested in T041.
- [X] T057 [US2] Create `server/notification/src/notification/NotificationEnvelopes.java`, which builds the `SubscriberNotified` envelope bytes for a `notification` row (`event_id = notification_id`, `routing_key = subscriber_id`, `correlation_id = sale_id`, `producer = notification`, `occurred_at_ms` = the selection time). Create `server/notification/src/notification/ChargingTracker.java`, holding the last confirmed send time per channel and `isCharged(attemptStartedAt, subscriberId)`, which is true when any other channel confirmed a send after `attemptStartedAt`.
- [X] T058 [US2] Create `server/notification/src/notification/NotificationDispatcher.java` per the dispatch table of [contracts/notification-delivery.md](contracts/notification-delivery.md#dispatch):
  - immediate intake, plus a sweep every `NOTIFICATION_SWEEP_MS`;
  - per-subscriber lanes with `NOTIFICATION_LANE_IN_FLIGHT` in flight, served round-robin;
  - provisioning of channels that are not ready;
  - batched marking every 20 ms or 200 IDs, after `FaultHooks.hit("notification.delivery.afterSend")`;
  - batched failure bookkeeping with the backoff;
  - parking on the `NOTIFICATION_PARK_AFTER_CHARGED`-th charged failure, with an alarm;
  - a stall alarm after 60 s with no confirmed send while rows are pending.
- [X] T059 [US2] Create `server/notification/src/notification/ChannelFaults.java`, a `RecordSender` decorator that applies `notification.channel.reject:<id>` (the returned future fails immediately) and `notification.channel.stall:<id>` (the future fails with a timeout after 10 s) by reading `FaultHooks`' property on each send. It is applied only to channel sends.
- [X] T060 [US2] Wire everything in `server/notification/src/notification/NotificationMain.java`: `KnownSkus`/`CatalogSkuConsumer`, the stores, the index rebuild at startup, `SubscriptionService`, the HTTP handler, `SaleSelectionHandler`/`SaleCompletedConsumer`, the dispatcher with `ChannelFaults` around a lazy `KafkaRecordSender` (`acks=all`, idempotence, `linger.ms=5`, `delivery.timeout.ms=10000`, `max.block.ms=1000`), and `KafkaChannelProvisioner`, keeping the T015 shutdown order.
- [X] T061 [US2] Create `server/notification/src/database/NotificationStatusQueries.java` and `server/notification/src/api/OpsHttpHandler.java` with `GET /notifications/ops/status`, returning every field of the contract's ops table (`subscriptionVersion`, `indexVersion`, `activeSubscribers`, `catalogLoaded`, `salesSelected`, notifications by state, charged and uncharged failures, `parkedEvents`, `alarms`, `saleConsumerLag`, `lastConfirmedSendAt`), and print the same fields in the 10-second status line.
- [X] T062 [US2] Write `spec/notification-openapi.yaml` (OpenAPI 3.0.3, `info.version: 1.0.0`) with `POST /notifications/subscribers`, `GET /notifications/subscribers/{subscriberId}`, the subscriber view schema, and the error schema, matching T043.
- [X] T063 [US2] Run `server/run-tests.sh unit database contract integration messaging performance architecture` and confirm that T036–T046 pass with no 006 regression.

**Checkpoint**: an end-to-end notification path exists; one subscriber's channel fault does not affect the others.

---

## Phase 5: User Story 3 - Reconcile Sales and Notifications Under Load With Fixed Subscriptions (Priority: P1)

**Goal**: The subscriber test application registers seeded subscribers before the Load Client runs, records every receipt, and the reconciler proves exactly-once delivery after the drain (FR-019–FR-026, FR-028 report, SC-002, SC-003, SC-013).

**Independent Test**: Quickstart scenario D: `server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -Workload default` ends with `reconciliation.json` `verdict: PASS`, and a second run with the same seed produces a byte-identical `snapshot.json`. Quickstart scenario C: every injected mismatch is detected.

### Tests for User Story 3 ⚠️

- [X] T064 [P] [US3] Write `subscriber-client/test/AssignmentDeterminismTest.java`:
  - the same seed and settings give a byte-identical `snapshot.json`;
  - subscriber *i*'s SKUs are identical for N = 10 and N = 100;
  - `uniform` gives K distinct SKUs from the catalog;
  - `popularity` gives K distinct SKUs biased toward low ranks;
  - `--assignments` overrides the seeded assignment;
  - identities are `sub-0001` … `sub-N`.
- [X] T065 [P] [US3] Write `subscriber-client/test/ReceiptWriterTest.java`:
  - one JSON line per record, with `subscriberId`, `notificationId`, `saleId`, `sku`, `quantity`, `completedAtMs`, `receivedAtMs`, `channelOffset`, and `redelivery`;
  - `redelivery` is true for a repeated `notificationId`;
  - after reopening, the resume offset is the highest `channelOffset` + 1, and earlier IDs are still known;
  - flushing happens after each batch.
- [X] T066 [P] [US3] Write `subscriber-client/test/ReconcilerTest.java` and `subscriber-client/test/ReconcileFaultInjectionTest.java` (SC-003). From a synthetic passing run directory (exports and receipts), the clean copy passes. Each injection must fail with exactly its category and key:
  - a removed `DELIVERED` row (missing, not delivered);
  - a removed receipt (missing, delivered but not received);
  - an extra receipt (unexpected);
  - a wrong quantity (quantity mismatch);
  - a second identity for one key (duplicate);
  - a selection with a different version (selection mismatch);
  - a history row above the ready version (fixed-subscription violation);
  - a `PARKED` row (parked);
  - a missing `SaleCompleted` row (sale-event check).

  Also check that redeliveries are counted without failing.
- [X] T067 [P] [US3] Write `subscriber-client/test/NotificationReportTest.java`:
  - nearest-rank p50/p95/p99 over the first receipts;
  - never-received notifications are excluded from the percentiles and counted as missing;
  - the active-window rates;
  - `drain.drainSeconds`, measured from the final completion to the last first receipt, with `deadlineMet` against 60 s;
  - JSON field names exactly as in [contracts/subscriber-client-and-reconciliation.md](contracts/subscriber-client-and-reconciliation.md#notification-report-notification-reportjson-rendered-as-notification-reportmd).

### Implementation for User Story 3

- [X] T068 [US3] Create `subscriber-client/build.sh` (`javac --release 21 -d out` over `src/*.java`, with the classpath `../server/out/contracts.jar` and `../server/lib/messaging/*`, failing clearly if they are missing), `subscriber-client/run.sh <Main> [args]`, and `subscriber-client/run-tests.sh` (compile `test/*.java` and run them with `../server/lib/junit-platform-console-standalone-1.11.0.jar`).
- [X] T069 [P] [US3] Create `subscriber-client/src/SubscriberConfig.java`, parsing every option in the `SubscriberMain` table of the contract with its defaults (`--subscribers` 100, `--skus-per-subscriber` 20, `--sku-distribution` uniform, `--id-prefix` sub, `--base-url` `http://localhost:8080`, `--bootstrap` `127.0.0.1:19092`, `--topic-prefix` empty, `--out` required, `--resume`, `--stop-file`, `--assignments`, and `--seed`, which is generated with `SecureRandom` and recorded when absent).
- [X] T070 [P] [US3] Create `subscriber-client/src/Assignment.java`: sorted catalog SKUs; per subscriber, a `SplittableRandom(mix64(seed + i))` using SplitMix64's finalizer; a partial Fisher–Yates draw for `uniform`; weighted sampling without replacement with Zipf weights 1/rank for `popularity`; and an explicit file for `--assignments`.
- [X] T071 [P] [US3] Create `subscriber-client/src/RunFiles.java`, writing `snapshot.json` (seed, settings, catalog SHA-256, and subscribers sorted by ID, with stable key order and no timestamps, so it is byte-identical) and `ready.json` (`readyAt`, the maximum `subscriptionVersion`, and per-subscriber versions).
- [X] T072 [US3] Create `subscriber-client/src/Registrar.java`, using `java.net.http.HttpClient` against `/notifications/subscribers`: 8 parallel registrations; up to 10 retries of 500 responses with 200 ms backoff; failure of the run on any 4xx; then polling `GET /notifications/subscribers/{id}` until `channelReady` is true for all, for at most 60 s.
- [X] T073 [US3] Create `subscriber-client/src/ReceiptWriter.java` and `subscriber-client/src/StatusWriter.java` (`status.json` every 1 s with per-subscriber `received`, `redeliveries`, and `lag`, plus totals, written atomically through a temporary file and a rename).
- [X] T074 [US3] Create `subscriber-client/src/SubscriberThread.java`: one `KafkaConsumer` (`group.id` unset, `enable.auto.commit=false`, `auto.offset.reset=earliest`) manually assigned to partition 0 of its channel topic. On `--resume`, it seeks to `ReceiptWriter.resumeOffset()`. It decodes `EventEnvelope` and `SubscriberNotified`, records receipts with `receivedAtMs = System.currentTimeMillis()`, and records undecodable records as `undecodable` lines. Lag is computed from `endOffsets`.
- [X] T075 [US3] Create `subscriber-client/src/SubscriberMain.java`, implementing the lifecycle of the contract: catalog through `GET /items`, the snapshot, registration (skipped on `--resume`), waiting for `channelReady`, starting the threads, `ready.json`, running until the stop file appears, and a clean shutdown with a final flush. The exit code is non-zero on registration failure.
- [X] T076 [US3] Create `db/export-notification-evidence.ps1 -RunDir DIR`, with the SQL in `db/transaction/export-sales.sql`, `db/transaction/export-sale-events.sql`, and `db/notification/export-evidence.sql`. Each part is run with its owner's credentials only, through `db/mysql-tools.ps1`, writing `sales.tsv`, `sale-events.tsv`, `selections.tsv`, `notifications.tsv`, `history.tsv`, and `parked-events.tsv` to `DIR/exports/`. `sales.tsv` aggregates `accepted_scan` per SKU for COMPLETED transactions. Sale-event lines are decoded by `subscriber-client/run.sh DecodeSaleEvents` (create `subscriber-client/src/DecodeSaleEvents.java`).
- [X] T077 [US3] Create `subscriber-client/src/Reconciler.java` and `subscriber-client/src/ReconcileMain.java` (`--run-dir`, `--mode=fixed|history`, `--report-out`), implementing every check of the reconciliation table in `fixed` mode and failing with `Unsupported` for `history` until T092. The output is `reconciliation.json` with `verdict`, `mode`, `counts`, and `samples` (the first 20 keys per category), and the exit code is non-zero on FAIL.
- [X] T078 [US3] Create `subscriber-client/src/NotificationReport.java`, with `ReconcileMain --report-out` writing `notification-report.json` and `notification-report.md` with every field of the contract's report schema. It takes the backlog samples from `DIR/backlog.tsv`, written by the harness.
- [X] T079 [US3] Create `server/benchmark-notification.ps1` with the parameters `-Subscribers`, `-Seed`, `-SkusPerSubscriber`, `-Workload default|stress`, `-RunName`, `-DrainSeconds` (120), `-IdleCpuCores`, and `-BlockingProcess`, implementing harness steps 1–4 and 6–9 of [contracts/deployment-and-evidence.md](contracts/deployment-and-evidence.md#harness-serverbenchmark-notificationps1). The `-Fault` parameter is accepted and rejected with "not implemented" until T120. It samples every 2 s into `backlog.tsv`: Transaction `saleEventsUnpublished`, the `notification-sale-completed` lag, Notification pending, and channel lag from `status.json`. It writes everything under `load-client/reports/notification-service/<RunName>/`, with the Load Client report copied unmodified as `load-client-report.json`. Reuse the helper functions of `server/benchmark-event-based.ps1`, moving them into a dot-sourced `server/benchmark-common.ps1` if more than three would be duplicated.
- [X] T080 [US3] Run `subscriber-client/run-tests.sh` and confirm that T064–T067 pass. Execute quickstart scenarios B, C, and D (one default run with 100 subscribers, plus a second run with the same seed for the snapshot comparison) into `load-client/reports/notification-service/us3-smoke/`, and record the outcomes in its `README.md`.

**Checkpoint**: the main load test reconciles end to end. This is the core acceptance gate of the feature.

---

## Phase 6: User Story 4 - Create, Update, and Cancel Subscriptions With Commit-Time Effect (Priority: P2)

**Goal**: Updates, cancellation, re-registration, rejected requests, and failed changes follow the Recipient Selection Rule, verified against the recorded history in a separate scenario (FR-006–FR-008, FR-010–FR-012, FR-022, FR-027, SC-004).

**Independent Test**: Quickstart scenario E: `ChangeScenarioMain` followed by `ReconcileMain --mode=history` passes. Every selection equals the history replayed to its version, every sale's version is at least the version confirmed before its Complete, and failed changes appear nowhere.

### Tests for User Story 4 ⚠️

- [X] T081 [P] [US4] Write `server/tests/unit/notification/SubscriptionChangeRulesTest.java`:
  - add only, remove only, and add plus remove;
  - adding held SKUs and removing absent ones is a no-op;
  - a removal that would leave zero SKUs is `LAST_SKU`;
  - a change to a cancelled subscriber is `SUBSCRIBER_CANCELLED`;
  - cancelling twice is a no-op;
  - registering after a cancel is a new `REGISTER`;
  - an SKU in both lists is `INVALID_REQUEST`;
  - the validation precedence is 400 → 404 → 409 → `UNKNOWN_SKU`.
- [X] T082 [P] [US4] Extend `server/tests/database/JdbcSubscriptionStoreTest.java` for `UPDATE` and `CANCEL`:
  - `subscription_change` versions are contiguous;
  - `resulting_skus` is correct;
  - `subscription` rows match `resulting_skus`;
  - a no-op writes no row;
  - `notification.subscription.beforeCommit:throw` leaves no row, the clock unchanged, and the index untouched.
- [X] T083 [P] [US4] Write `server/tests/database/SelectionLockingTest.java`, with two connections on the test schema:
  - a selection holding the clock `FOR SHARE` blocks a change's `FOR UPDATE` until it commits;
  - a selection started after a change commits records the change's version and sees its rows;
  - two concurrent changes get consecutive versions in commit order;
  - two selections do not block each other.
- [X] T084 [P] [US4] Write `server/tests/integration/SubscriptionChangeConsistencyTest.java`, covering spec US4 acceptance scenarios 1–8 on `InProcessCheckout` + `InProcessNotification`:
  - a confirmed register, add, remove, or cancel before a Complete is reflected in that sale;
  - an already-selected notification survives a cancellation;
  - a failed change has no effect;
  - concurrent changes during sales leave every selection equal to the history replayed to its version;
  - after a restart, the rebuilt index equals the database;
  - with two `InProcessNotification` instances sharing one schema, each selection catches up to the locked version.
- [X] T085 [P] [US4] Extend `server/tests/contract/NotificationSubscribersEndpointTest.java` with every row for `PATCH /notifications/subscribers/{id}` and `DELETE /notifications/subscribers/{id}`: 200 with a new version, 200 no-op, 409 `LAST_SKU`, 409 `SUBSCRIBER_CANCELLED`, 404 `SUBSCRIBER_NOT_FOUND`, 400 `INVALID_REQUEST`, 400 `UNKNOWN_SKU`, and an idempotent repeated DELETE.
- [X] T086 [P] [US4] Write `subscriber-client/test/HistoryReconcileTest.java`, covering history mode on synthetic exports:
  - a selection that matches the replayed history passes;
  - a selection missing a subscriber, or containing an extra one, fails as a selection mismatch;
  - a sale whose version is below the version confirmed before its Complete, per `changes.jsonl`, fails the causality check;
  - a failed change recorded in `changes.jsonl` that appears in the history fails.

### Implementation for User Story 4

- [X] T087 [US4] Extend `server/notification/src/database/JdbcSubscriptionStore.java` with `update(subscriberId, add, remove)` and `cancel(subscriberId)`, following the same change transaction (`UPDATE` and `CANCEL` rows, `subscription` row inserts and deletes, `subscriber.updated_version`, and `status = 'CANCELLED'` with `skus` cleared on cancel).
- [X] T088 [US4] Extend `server/notification/src/notification/SubscriptionService.java` with `update` and `cancel`, following T081's rules and applying the change to the index only after commit. Make the index's catch-up path the single way to recover from ambiguous commits and from changes committed by other processes.
- [X] T089 [US4] Extend `server/notification/src/api/NotificationHttpHandler.java` with `PATCH` and `DELETE` per the contract, and extend `spec/notification-openapi.yaml` with both operations and their errors.
- [X] T090 [US4] Add `POST /notifications/ops/faults` to `server/notification/src/api/OpsHttpHandler.java`. When `NOTIFICATION_TEST_FAULTS=1`, it sets `System.setProperty(FaultHooks.PROPERTY, value)`, or clears it for an empty value, and answers `{"faults": "<value>"}`. Otherwise it answers 404. Add `server/tests/contract/OpsFaultsEndpointTest.java` for both modes.
- [X] T091 [US4] Create `subscriber-client/src/ChangeScenarioMain.java`, implementing the steps of research N12 with the options in the contract:
  - register 10 subscribers;
  - for each of register, add, remove, cancel, and re-register: confirm, then complete a sale of an affected SKU through `POST /transactions`, `/items`, and `/complete`, recording `(confirmedVersion, transactionId)`;
  - a burst of 50 random changes, with `--with-load` waiting for an external Load Client;
  - three failed changes through `--faults-endpoint`, using `notification.subscription.beforeCommit:throw`;
  - one request of each kind that FR-006 rejects;
  - writing `changes.jsonl`.
- [X] T092 [US4] Implement `--mode=history` in `subscriber-client/src/Reconciler.java`: replay `history.tsv` to each selection's version, require each recorded selection to equal it, apply the causality check from `changes.jsonl`, require failed changes to be absent, then use the recorded selections as the owed set for the delivery and receipt checks.
- [X] T093 [US4] Run the US4 tests (`server/run-tests.sh unit database contract integration` and `subscriber-client/run-tests.sh`). Execute quickstart scenario E twice, once alone and once with `--with-load` and a parallel default Load Client, into `load-client/reports/notification-service/change-scenario/`, with `README.md` recording the SC-004 results.

**Checkpoint**: subscription management is complete and verified against the selection rule.

---

## Phase 7: User Story 5 - Keep Notifications Exactly Once Through Duplicates, Failures, and Restarts (Priority: P2)

**Goal**: Every row of the fault and recovery matrix yields exactly one effective notification per owed key. Failures are visible, parked after the limit, and retryable (FR-015–FR-018, SC-005).

**Independent Test**: Quickstart scenario F items 1–6 each end with a passing reconciliation, except item 6, where the malformed event is parked and reported by design. Broker downtime produces only uncharged failures and no parked notifications.

### Tests for User Story 5 ⚠️

- [X] T094 [P] [US5] Write `server/tests/integration/NotificationCrashMatrixTest.java`, with one test per row of the fault and recovery matrix in [contracts/notification-delivery.md](contracts/notification-delivery.md#fault-and-recovery-matrix), using `FaultInjection` with `transaction.sale.beforeT3Commit`, `transaction.sale.afterT3Commit`, `notification.selection.beforeCommit`, `notification.selection.afterCommit`, and `notification.delivery.afterSend`, plus a duplicated `SaleCompleted` and a lost publish confirmation. Each test asserts one `sale_selection`, one notification row per key, and the expected redelivery count after recovery.
- [X] T095 [P] [US5] Write `server/tests/unit/notification/ChargingRuleTest.java`:
  - a failure while another channel confirmed a send after the attempt started is charged;
  - a failure with no other confirmation is uncharged;
  - the 8th charged failure parks the row;
  - uncharged failures never park;
  - the stall alarm is raised after 60 s with no confirmed send while rows are pending, and cleared on the next confirmation.
- [X] T096 [P] [US5] Write `server/tests/database/ParkingAndOperatorRetryTest.java`: `retryParked(all)` and `retryParked(subscriber)` return the exact counts, reset `charged_failures`, increment `operator_retries`, set `next_attempt_at = now`, and keep `notification_id`; `DELIVERED` rows are never touched.
- [X] T097 [P] [US5] Write `server/tests/messaging/NotificationBrokerRestartTest.java` (live Kafka): with 5 subscribers and sales in flight, stop the broker for 30 s through `KafkaTestSupport`, then start it. Every owed notification is delivered exactly once in effect, `charged_failures` is 0 on every row, no row is `PARKED`, and Transaction's `saleEventsUnpublished` returns to 0.
- [X] T098 [P] [US5] Write `server/tests/messaging/SaleEventParkingLiveTest.java` (live Kafka): `PublishRawRecord` sends a malformed `SaleCompleted` and an unsupported-version record to `selfcheckout.sale-completed.v1`. Both are parked in Notification's `dead_letter_event` after 5 attempts, `ops/status` shows `parkedEvents = 2` and an alarm, and valid sales on other and the same partitions keep flowing.
- [X] T099 [P] [US5] Write `server/tests/integration/TransactionRestartSaleEventsTest.java`: completions committed while the dispatcher is stopped, followed by a Transaction crash and restart, are published by the sweep and selected once each (US5 scenario 5).
- [X] T100 [P] [US5] Write `subscriber-client/test/SubscriberResumeLiveTest.java` (live Kafka, skipped when `KAFKA_TEST_BOOTSTRAP` is unset): publish 100 records to a test channel, stop `SubscriberThread` after 40, publish 60 more, and resume. All 160 are recorded once, plus at most one poll batch flagged as redeliveries (US5 scenario 6).

### Implementation for User Story 5

- [X] T101 [US5] Add `GET /notifications/ops/parked?subscriberId=&limit=` and `POST /notifications/ops/parked/retry` (body `{"subscriberId": optional}`, answering `{"retried": n}`) to `server/notification/src/api/OpsHttpHandler.java`, using `JdbcNotificationStore.retryParked` and listing parked events from `dead_letter_event`. Retried rows re-enter `NotificationDispatcher` through the sweep.
- [X] T102 [US5] Harden recovery at startup in `server/notification/src/notification/NotificationDispatcher.java` and `NotificationMain.java`:
  - on start, the sweep immediately takes all due `PENDING` rows;
  - channels with `channel_ready = FALSE` are re-provisioned before sending;
  - alarms for parked rows and parked events are restored from the database counts, so they survive a restart.

  Extend `server/tests/integration/NotificationCrashMatrixTest.java` with a restart while rows are parked.
- [X] T103 [US5] Run `server/run-tests.sh integration messaging database unit` and `subscriber-client/run-tests.sh` (the US5 tests). Execute quickstart scenario F items 1–6 with `server/benchmark-notification.ps1 -Workload default -Subscribers 100`, injecting each fault manually through `ops/faults` or the 006 restart procedures, into `load-client/reports/notification-service/fault-matrix/<item>/`, with `README.md` recording each reconciliation verdict and redelivery count.

**Checkpoint**: exactly-once effective notification holds through every injected fault.

---

## Phase 8: User Story 6 - Keep Checkout, Analytics, and the Client Interface Unchanged (Priority: P2)

**Goal**: The seven routes, the OpenAPI document, the Load Client, and every 006 guarantee hold whether Notification is healthy, slow, backlogged, or stopped (FR-031, FR-033, SC-008, SC-009).

**Independent Test**: Quickstart scenario G: the full server suite passes; the hash check is identical; a default Load Client run with Notification stopped has zero errors, and reconciliation passes after Notification starts.

### Tests for User Story 6 ⚠️

- [X] T104 [P] [US6] Write `server/tests/integration/NotificationOutageCheckoutTest.java`:
  - with Notification stopped, Start, Scan, Lookup, and Complete behave as in 006, with the same responses and the same commit counts;
  - with a slow Notification consumer (a 2 s `FaultHooks` delay in selection), Complete latency is unchanged within the test tolerance and `saleEventsUnpublished` stays 0, because the events are on the broker;
  - after Notification starts, every sale is selected once.
- [X] T105 [P] [US6] Write `server/tests/integration/AnalyticsUnaffectedByNotificationsTest.java`: for one fixed scan stream, the published windows and ranks are identical with Notification running at 100 subscribers and without it.
- [X] T106 [P] [US6] Write `server/tests/architecture/ClientAndContractUnchangedTest.java`: every file under `load-client/src/` and `spec/self-checkout-openapi.yaml` is byte-identical to `git show 4a91686:<path>`; `deploy/nginx.conf` differs from `4a91686` only by the added upstream and `/notifications/` location.
- [X] T107 [P] [US6] Extend `server/tests/integration/GatewayServiceOutageTest.java`: with Notification unreachable, `/notifications/**` returns the gateway's existing 502 body, and all seven existing routes are unaffected.

### Implementation for User Story 6

- [X] T108 [US6] Fix any regression that T104–T107 reveal in `server/notification/src/**` or `server/transaction/src/**` without changing 006 behavior or assertions, and record any changed 006 test, with its reason, in `server/tests/COVERAGE-LEDGER.md` under a new "Feature 007" section. The expected entry is the T3 budget only.
- [X] T109 [US6] Run the full `server/run-tests.sh` (no selector) with `STRICT_LIVE_GATES=1`. Execute quickstart scenario G (Load Client default with Notification stopped, then Notification started, drain, and `ReconcileMain --mode=fixed`) into `load-client/reports/notification-service/us6-unchanged/`, with `README.md`.

**Checkpoint**: nothing a client or Analytics can observe has changed.

---

## Phase 9: User Story 7 - Operate and Reset Notification Service Independently (Priority: P2)

**Goal**: Notification builds, starts, stops, restarts, resets, and seeds on its own and reaches only its own data, and one baseline resets everything (FR-034–FR-036, SC-012).

**Independent Test**: Spec US7 acceptance scenarios 1–4: independent build and start, credential isolation in both directions, the baseline contents, and a restart during a workload followed by a passing reconciliation.

### Tests for User Story 7 ⚠️

- [X] T110 [P] [US7] Extend `server/tests/integration/DatabaseIsolationTest.java`: `cs6510_notification_svc` is denied every statement on the other three schemas, and each of the other three users is denied on `cs6510_notification`.
- [X] T111 [P] [US7] Extend `server/tests/integration/IndependentServiceDeploymentTest.java`: `server/build.sh notification` builds without building the service jars, and `server/run-notification.sh` starts with only the broker and MySQL running, answering `ops/status`.
- [X] T112 [P] [US7] Extend `server/tests/integration/ResetBaselineTest.java`: after `db/reset-baseline.ps1 -RunDir <tmp>`, Notification has clock 0 and empty tables, no `selfcheckout.notify.*` topic exists, exactly five fixed topics exist with zero end offsets, the run directory is gone, and 006's baseline assertions still hold.
- [X] T113 [P] [US7] Extend `server/tests/integration/RestartDuringWorkloadTest.java` with a Notification restart in the middle of a 30-second workload with 10 subscribers. Afterwards, every owed notification is delivered once in effect, and the other services served without error.

### Implementation for User Story 7

- [X] T114 [US7] Extend `deploy/kafka/status.ps1` with the `notification-sale-completed` group lag. Extend `server/benchmark-event-based.ps1` and `server/benchmark-compare.ps1` port checks to include 8084, so 006 harnesses refuse to run beside a stray Notification process.
- [X] T115 [P] [US7] Add a Notification section to `server/README.md` (build, run, ports, the configuration table, the connection budget of 64 + 48 + 10 + 20 = 142, status fields, ops endpoints, and `NOTIFICATION_DB_PASSWORD` setup), and update `deploy/kafka/README.md` (the sale topic, channel topics created by Notification, and their removal by the reset).
- [X] T116 [P] [US7] Write `subscriber-client/README.md`: build, the options of `SubscriberMain` and `ChangeScenarioMain`, run-directory files, `ReconcileMain` modes and exit codes, the report fields, and its relationship to the unchanged Load Client.
- [X] T117 [US7] Run the US7 tests, then execute the baseline and independent-start checks of quickstart §1, recording them in `load-client/reports/notification-service/us7-operations.md`.

**Checkpoint**: Notification is operated like the other three services.

---

## Phase 10: User Story 8 - Produce Notification Performance Evidence and the Updated Diagram (Priority: P3)

**Goal**: Genuine evidence at four subscriber counts, the two standard reports with 100 subscribers plus their notification reports, the isolation runs, and the diagram (FR-028, FR-029, FR-037, SC-006, SC-007, SC-010, SC-011, SC-014).

**Independent Test**: `load-client/reports/notification-service/README.md` links every committed report. Each run's `reconciliation.json` passes, and the isolation medians meet SC-006. The diagram agrees with `TopicNames` and `EventTypes`.

### Tests for User Story 8 ⚠️

- [ ] T118 [P] [US8] Extend `server/tests/architecture/ArchitectureDiagramConsistencyTest.java` to read `docs/architecture/notification-service.md` and require four services with their databases, the five fixed topics with producer and consumer edges matching `EventTypes` and `TopicNames`, the channel family `selfcheckout.notify.<subscriberId>.v1` from Notification to the subscriber test application, and no synchronous service-to-service edge.
- [ ] T119 [P] [US8] Write `subscriber-client/test/IsolationSummaryTest.java` for the `IsolationSummary` tool of T125: the median of three `endToEndExcluding.p95` values per mode, the ratio against the healthy median, and a pass when it is ≤ 1.20 with zero missing or duplicate notifications for the 99.

### Implementation for User Story 8

- [ ] T120 [US8] Implement `-Fault none|reject|stall` and `-FaultSubscriber` in `server/benchmark-notification.ps1`, so step 5 and the second drain of step 7 work:
  - start Notification with `NOTIFICATION_TEST_FAULTS=1`, but only for faulted runs;
  - set `notification.channel.<fault>:<id>` before the Load Client starts, and clear it after the first drain;
  - call `POST /notifications/ops/parked/retry`, then wait for the second drain;
  - record the fault in `run.faults`.

  Make `NotificationReport` compute `endToEndExcluding` for the faulted subscriber, over the first drain's receipts of the other subscribers.
- [ ] T121 [P] [US8] Write `docs/architecture/notification-service.md` (a Mermaid flowchart: gateway → four services; each service → its own database; Kafka with the five fixed topics and the channel family; producer and consumer edges as in the event catalogs; the subscriber test application with N threads; the Load Client), and export it to `docs/architecture/notification-service.svg`.
- [ ] T122 [US8] Run CP2 (full system with 100 subscribers): `server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111`, default twice and stress once, under `load-client/reports/notification-service/cp2/`, and compare with CP1 in `checkpoints.md`, investigating any drop of more than 5% in `totalTransactions`.
- [ ] T123 [US8] Optional CP3: an interleaved A/B with `server/benchmark-compare.ps1` of `NOTIFICATION_SALE_CONSUMER_THREADS` 4 vs 8, `NOTIFICATION_LANE_IN_FLIGHT` 32 vs 64, and a marking interval of 20 ms vs 50 ms, each default × 2 per side, under `cp3-*/`. Adopt a setting only if it improves `totalTransactions` or p95 notification latency by ≥ 5% with no reconciliation failure, and record the decision in `checkpoints.md`.
- [ ] T124 [US8] Run the scaling evidence: `server/benchmark-notification.ps1 -Workload default -Seed 20261111 -SkusPerSubscriber 20` with `-Subscribers` 10, 25, 50, and 100, each after a fresh baseline, into `load-client/reports/notification-service/scaling/{10,25,50,100}/`.
- [ ] T125 [US8] Create `subscriber-client/src/IsolationSummary.java` (it reads `isolation/*/notification-report.json` and writes `isolation/README.md` with the per-mode medians, the ratios, and the SC-006 verdict). Run the isolation evidence, 3 × `-Fault none`, 3 × `-Fault reject`, and 3 × `-Fault stall`, interleaved in the order healthy, reject, stall, with `-Subscribers 100 -Seed 20261111 -FaultSubscriber sub-0001 -Workload default`, into `isolation/{healthy,reject,stall}-{1,2,3}/`, then run `IsolationSummary`.
- [ ] T126 [US8] Run CP4, the official standard reports with 100 subscribers, each after a fresh baseline:
  - `-Workload default` → copy its Load Client report to `load-client/reports/notification-service/default-report.json` and its notification report and reconciliation to `default-notification-report.{json,md}` and `default-reconciliation.json`;
  - `-Workload stress` → `stress-report.json`, `stress-notification-report.{json,md}`, and `stress-reconciliation.json`.

  Run `db/validate-invariants.ps1` after each, and record it.
- [ ] T127 [US8] Write `load-client/reports/notification-service/README.md`:
  - an index of every committed run;
  - a table per subscriber count of sales/s, delivered/s, received/s, p50/p95/p99, failures, and maximum backlog;
  - the default and stress `totalTransactions`, throughput, error rates, and p95/p99 compared with `load-client/reports/event-based-architecture/default-report.json` and `stress-report.json`;
  - the SC-006 and SC-007 results;
  - an SC-001–SC-014 status table, each row linking its evidence.

**Checkpoint**: all deliverables are committed, genuine, and reconciled.

---

## Phase 11: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation accuracy, and compliance review.

- [ ] T128 [P] Update `specs/007-notification-service/quickstart.md` so every command matches what was implemented: scenario C is `subscriber-client/run-tests.sh`, scenario G is `server/run-tests.sh` with `STRICT_LIVE_GATES=1`, and the actual script parameters are used. Record a full quickstart pass in `load-client/reports/notification-service/quickstart-validation.md`.
- [ ] T129 Run the final release gate: `server/build.sh all`, `server/build-tests.sh`, `STRICT_LIVE_GATES=1 server/run-tests.sh`, and `subscriber-client/run-tests.sh` on an idle machine. Save the summaries to `load-client/reports/notification-service/final-gate-tests.txt`, and confirm zero failures and zero skipped live suites.
- [ ] T130 Perform the constitution compliance review: re-check every row of plan.md's *Constitution Check* (v7.0.0, gates I–XI, constraints, workflow) against the code and the evidence, and append an *Implementation Notes* section to `specs/007-notification-service/plan.md` recording where the implementation refined the plan and confirming that no gate changed.
- [ ] T131 Re-validate `specs/007-notification-service/checklists/requirements.md` against the final spec, and make sure every FR-001–FR-038 is covered by at least one passing test or committed evidence, listing the mapping in `load-client/reports/notification-service/README.md`'s SC table or an appendix.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: T001 first; T002 before any code change. T003–T006 then run in parallel.
- **Foundational (Phase 2)**: depends on Setup. T007 → T008 (uses the new topic) → T019. T010 → T011, T012 → T013. T014 → T015. It blocks every story.
- **US1 (Phase 3)**: depends on Foundational only. It is the MVP, and CP1 (T035) closes it.
- **US2 (Phase 4)**: depends on Foundational and on US1, because it consumes `SaleCompleted` end to end; its unit and database tests can start right after Foundational.
- **US3 (Phase 5)**: depends on US2 (registration, delivery, status).
- **US4 (Phase 6)**: depends on US2 (change transaction, index), and on US3 for history-mode reconciliation and the scenario driver.
- **US5 (Phase 7)**: depends on US2, and on US3 for the scenario F runs.
- **US6 (Phase 8)**: depends on US1 and US2; T109 also uses US3's reconciler.
- **US7 (Phase 9)**: depends on Foundational and US2; T113 uses US3's tooling.
- **US8 (Phase 10)**: depends on US3 (harness), US4 (fault endpoint T090), and US5 (operator retry T101).
- **Polish (Phase 11)**: depends on all stories.

### User Story Dependency Graph

```text
Setup → Foundational → US1 → US2 → US3 ─┬→ US4 ─┐
                                        ├→ US5 ─┼→ US8 → Polish
                                        ├→ US6  │
                                        └→ US7 ─┘
```

### Within Each User Story

- Tests are written first and must fail before implementation.
- Ports and stores come before business services, business services before handlers and consumers, and those before composition-root wiring.
- Each story ends with its test run and quickstart scenario before the next story starts.

### Parallel Opportunities

- Setup: T003–T006.
- Foundational: T009, T011, T016, T017 alongside the T007/T008 and T010/T012 chains.
- US1: tests T020–T025 together.
- US2: tests T036–T046 together. After T048 and T049, T051 (provisioner), T053 (selection store), T056 (notification store), and T057 (envelopes and charging) touch different files and can run in parallel.
- US3: tests T064–T067 together; T069–T071 together.
- US4: tests T081–T086 together.
- US5: tests T094–T100 together.
- US6: tests T104–T107 together. US7: tests T110–T113 together, plus T115 and T116.
- US6 and US7 can run in parallel after US3. US4 and US5 can run in parallel after US3, as long as T090 (US4) lands before T120 (US8).
- Benchmark tasks (T002, T035, T122–T126) never run in parallel with anything (Implementation Rule 5).

---

## Parallel Example: User Story 2

```bash
# All US2 tests together (different files):
Task: "SubscriptionIndexTest in server/tests/unit/notification/SubscriptionIndexTest.java"
Task: "SelectionPlanTest in server/tests/unit/notification/SelectionPlanTest.java"
Task: "JdbcSubscriptionStoreTest in server/tests/database/JdbcSubscriptionStoreTest.java"
Task: "JdbcSelectionStoreTest in server/tests/database/JdbcSelectionStoreTest.java"
Task: "NotificationSubscribersEndpointTest in server/tests/contract/NotificationSubscribersEndpointTest.java"

# Independent adapters after SubscriptionIndex and SubscriptionStore exist:
Task: "KafkaChannelProvisioner in server/notification/src/messaging/KafkaChannelProvisioner.java"
Task: "JdbcSelectionStore in server/notification/src/database/JdbcSelectionStore.java"
Task: "JdbcNotificationStore in server/notification/src/database/JdbcNotificationStore.java"
```

## Parallel Example: User Story 3

```bash
Task: "AssignmentDeterminismTest in subscriber-client/test/AssignmentDeterminismTest.java"
Task: "ReconcileFaultInjectionTest in subscriber-client/test/ReconcileFaultInjectionTest.java"
Task: "SubscriberConfig in subscriber-client/src/SubscriberConfig.java"
Task: "Assignment in subscriber-client/src/Assignment.java"
Task: "RunFiles in subscriber-client/src/RunFiles.java"
```

---

## Implementation Strategy

### MVP First (User Story 1)

1. Complete Phase 1 (constitution commit, CP0) and Phase 2 (registrations, schema, skeleton).
2. Complete US1: completed sales are recorded and published exactly once, and CP1 shows the cost.
3. **Stop and validate**: quickstart scenario A and the CP1 comparison.

### Incremental Delivery

1. **US1**: sale events.
2. **US2**: end-to-end notifications.
3. **US3**: the main load test reconciles. This is the core acceptance gate.
4. **US4 and US5**: subscription changes and fault tolerance.
5. **US6 and US7**: no regression, plus operability.
6. **US8**: evidence.
7. **Polish**.

Each story ends with passing tests and a recorded quickstart scenario. Per the standing instruction, finish each phase, commit it locally, and continue to the next without pausing.

---

## Notes

- [P] tasks touch different files and depend on no incomplete task.
- [Story] labels map tasks to spec user stories for traceability.
- Commit after each task or logical group, never in the middle of a benchmark.
- Evidence files are always genuine output, never edited by hand except the README summaries.
