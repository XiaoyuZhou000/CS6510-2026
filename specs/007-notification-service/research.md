# Research: Sale Notification Service with Subscriber Reconciliation

Phase 0 decisions for [plan.md](plan.md). Each decision states what was chosen, why, and what was rejected. The decisions build on feature 006's research (`specs/006-event-based-architecture/research.md`, cited as 006 D*n*), which stays in force unless a decision below replaces it.

The Technical Context contained no NEEDS CLARIFICATION item. The unknowns below come from the spec's *Planning Obligations* and from integrating with the 006 code.

## N1. Baseline and evolution path

**Decision**: start from feature 006 as committed at `4a91686` on `event-based-architecture`, which is also the tip of `feature/notification-service`. Add Notification Service as a fourth service in the 006 style: a plain JAR with `api`, business, `database`, and `messaging` layers; its own port (8084), schema, and credentials; and the shared `common/events` framework. Change Transaction Service in one place only: the T3 completion transaction.

**Rationale**: Principle I and the Architectural Constraints require incremental evolution, and the 006 framework (outbox dispatcher, consumer loop, parking, envelope codec, fault hooks, status reporter) already covers most of what Notification Service needs.

**Alternatives considered**:
- Putting notification logic inside Transaction Service: forbidden by Principle I (Transaction MUST NOT select recipients) and FR-004.
- A separate repository or framework for the new service: it would duplicate the event framework and break the comparable build.

## N2. Sale identity and the SaleCompleted event

**Decision**:
- **Sale identity** is the transaction ID. Each COMPLETED transaction is exactly one sale (FR-001), and the transaction ID is already stable, unique, and carried by every checkout event.
- **Event**: a new protobuf type `SaleCompleted` (schema version 1) in `sale_events.proto`, carrying `sale_id`, `transaction_id`, `attempt_number`, `completed_at_ms`, `station_id`, and `lines` (SKU and quantity, unique SKUs in ascending order). Quantity is the number of units of the SKU in the completed basket. See [contracts/sale-completed-event.md](contracts/sale-completed-event.md).
- **Where it is recorded**: inside T3 (`JdbcCheckoutAttemptStore.complete`). After the guarded `UPDATE transaction … COMPLETED` and the attempt `UPDATE`, T3 inserts one `outbox_event` row for `SaleCompleted` and commits once. T3 becomes three statements and one commit. After the commit, the existing `OutboxSignal` hands the event ID to Transaction's `OutboxDispatcher`, as T1 already does.
- **Event ID**: a name-based UUID (version 3) of `sale:<transaction_id>`, so it is deterministic and fits `CHAR(36)`. `uq_outbox_event_id` therefore makes a second `SaleCompleted` for the same transaction impossible even if the guard failed (defense in depth for FR-003). `aggregate_key` is `sale:<transaction_id>`.
- **Lines source**: `CheckoutOutcomeHandler` builds the payload from the frozen basket in `TransactionStateCache` (the basket cannot change while an attempt is pending). On a cache miss, it reads the attempt's canonical `lines` snapshot with `CheckoutAttemptStore.find` before T3. Item count and lines are checked against each other; a mismatch is an integrity fault (006 FR-038), not a completion.
- Repeated Complete requests, redelivered `StockDeducted` events, and recovery all fail T3's guard and therefore insert nothing (FR-003).

**Rationale**: recording the event in the completing transaction is what Principle X requires. The transaction ID is the only identity that is already stable across retries, redelivery, and restarts. A deterministic event ID turns the at-most-once rule into a database constraint.

**Alternatives considered**:
- **Deriving sales from `StockDeducted`**: that event belongs to Inventory, and it marks the pivot, not durable COMPLETED in Transaction. It would also make Notification Service interpret the Saga.
- **A random UUID event ID**: correct only through T3's guard; the deterministic ID costs nothing and adds a second barrier.
- **Emitting the event from the response path**: not in the same local transaction; prohibited by Principle VII.

## N3. Topic and consumer group for completed sales

**Decision**: a new topic `selfcheckout.sale-completed.v1` with 16 partitions, `delete` cleanup with the 006 Windows segment pins, record key = transaction ID, producer Transaction, consumer Notification in group `notification-sale-completed` with 4 consumer threads. `deploy/kafka/create-topics.ps1` creates it with the other four, and `TopicNames` and `EventTypes` gain the entries.

**Rationale**: per-transaction keys keep one sale's events on one partition, which is all ordering requires: sales are independent of one another. Sixteen partitions match the checkout topics and allow more consumer threads later without a topic change. Four threads cover about 90 sales per second, the stress rate measured by 006, with headroom.

**Alternatives considered**:
- **Reusing `selfcheckout.checkout-outcome.v1`**: its consumers are Transaction's, and mixing producers on one topic breaks the one-producer-per-type documentation.
- **A single partition**: needlessly serializes selection.

## N4. Subscription model, history, and idempotent requests

**Decision**:
- **Tables** ([data-model.md](data-model.md)): `subscriber` (identity, status ACTIVE or CANCELLED, channel topic, versions), `subscription` (the current SKUs of active subscribers), `subscription_change` (the ordered history, one row per committed change), and `subscription_clock` (a single row holding the current history version).
- **Single ordered history** (FR-007, rule 3): every change transaction starts with `SELECT version FROM subscription_clock WHERE id = 1 FOR UPDATE`, applies its rows, inserts its `subscription_change` row at `version + 1`, sets the clock, and commits. The exclusive row lock serializes all changes, so version order equals commit order. Subscription changes are rare, so one global lock costs nothing measurable.
- **Effective at commit** (FR-007): the change is confirmed only after commit, and the response carries the new `subscriptionVersion`. A change that fails or rolls back writes nothing. A change whose commit outcome is unknown returns 500, and repeating the identical request is safe.
- **Idempotency** (FR-006): requests are compared with the current state. Registering an active subscriber with the same SKU set, adding SKUs it already holds, removing SKUs it does not hold, or cancelling a cancelled subscriber is a no-op that returns the current state and the version of the change that produced it, without a new history entry. No request ID is needed.
- **Operations**: register (with one or more SKUs), add and remove SKUs in one request (the result must keep at least one SKU), cancel, and re-register after cancellation. A cancelled subscriber keeps its identity and channel ([contracts/notification-http.md](contracts/notification-http.md)).

**Rationale**: a version counter guarded by a row lock is the simplest way to give every committed change a total order that both selections and verification can name. Storing the history makes every selection reproducible (rule 4).

**Alternatives considered**:
- **Timestamps as history points**: not unique, and they depend on clocks.
- **Per-subscriber versions**: they do not give a single point that covers every subscriber at once, which rule 3 needs.
- **Client-supplied request IDs for idempotency**: unnecessary, because comparing with the current state already makes every operation idempotent.

## N5. Recipient selection and the in-memory index

**Decision**: one local transaction per sale (the selection transaction), run by the `SaleCompleted` consumer:
1. `SELECT version FROM subscription_clock WHERE id = 1 FOR SHARE` returns `V`. The shared lock blocks any change from committing until the selection commits, so the state read next is exactly the state at `V` (rule 3), under REPEATABLE READ or READ COMMITTED alike.
2. Recipients come from the in-memory `SubscriptionIndex` (SKU → sorted subscriber IDs), an immutable snapshot tagged with its version. If its version is below `V`, the selection first loads `subscription_change` rows with `version > index.version` in version order and applies them (catch-up). An index version above `V` cannot occur, because the index only applies committed changes.
3. `INSERT INTO sale_selection` with the sale identity, the lines, `V`, and the counts. A duplicate key means the sale was already selected: the transaction rolls back and the event is acknowledged with no effect (FR-012, FR-015).
4. Multi-row `INSERT INTO notification`, at most 500 rows per statement, one row per selected (sale, SKU, subscriber).
5. Commit, then hand the new notification IDs to the dispatcher (N8). The consumer loop commits the offset only after this commit (006 D6).

The index is built at startup from `subscription` plus the clock value, and is updated after each change this process commits, in version order. It is never authoritative: every selection checks its version against the locked clock and catches up from the database, which also covers changes committed by another Notification process (FR-008). A failed change never reaches the index, because only committed history rows are applied.

**Rationale**: the shared lock gives an exact, recordable history point for each selection. Selections share the lock with one another, so they never block each other, and subscriptions are frozen during the main load test, so the catch-up path is not on the hot path there. The version check makes the index a pure acceleration structure, as Principle II requires.

**Alternatives considered**:
- **Querying `subscription` on every sale**: correct but costs a join per sale; the index removes it.
- **A lock-free optimistic check**, reading the clock before and after: it needs a retry loop and still needs a recorded version.
- **Selecting against the subscription state at the sale's completion time**: needs cross-service clock comparison, which correctness must never depend on.

## N6. SKU validation without reading Inventory

**Decision**: Notification Service validates SKUs against an in-memory set of known SKUs, filled by replaying the compacted `selfcheckout.catalog.v1` topic from offset 0 at startup, exactly as Transaction does (006 D9), through a manually assigned consumer with no group. Until a complete catalog snapshot has arrived, registration and SKU changes return `500 INTERNAL_ERROR` with no change. Selection never consults this set: the SKUs of a sale are already valid.

**Rationale**: FR-006 requires rejecting unknown SKUs, and Principle II forbids reading Inventory's data. The compacted topic already provides the full catalog to any late consumer. The set validates requests only and holds no acknowledged state, so it does not need to be durable.

**Alternatives considered**:
- **A durable catalog replica table**: it adds writes for every catalog publication with no benefit, because subscription changes cannot be accepted safely without a fresh catalog anyway.
- **Accepting any SKU string**: it violates FR-006.

## N7. Subscriber channels: one Kafka topic per subscriber

**Decision**: each subscriber's channel is its own single-partition topic, `selfcheckout.notify.<subscriberId>.v1` (with the test prefix), created by Notification Service with the Kafka `Admin` client when the subscriber is first registered. It uses the 006 Windows segment pins, `segment.index.bytes` of 1 MiB to bound memory-mapped index files across 100 or more topics, and no retention. Creation is idempotent (`TopicExistsException` is success). If creation fails, registration still commits, and the dispatcher retries provisioning before the first delivery. `auto.create.topics.enable` stays false. A cancelled subscriber keeps its topic; the baseline reset removes all channel topics by replacing the log directory (006 D13).

A notification is published to its subscriber's channel with key = subscriber ID, as an `EventEnvelope` whose `event_id` is the notification identity, `event_type` is `SubscriberNotified`, and `producer` is `notification` ([contracts/sale-completed-event.md](contracts/sale-completed-event.md)).

**Rationale**: a topic per subscriber is the most direct form of an independent channel (FR-009). A fault injected into one channel, or a backlog in one, affects no other subscriber's partition or consumer position. It stays on the single Kafka broker (Principle VIII). About 100 to 200 one-partition topics are well within a single node's limits.

**Alternatives considered**:
- **One shared topic, partitioned by subscriber**: subscribers would share partitions, so a stalled partition or a slow consumer would affect several of them, which weakens the isolation SC-006 measures.
- **HTTP push to the subscriber application**: not durable and not asynchronous without a queue; it would need a store-and-forward mechanism that Kafka already provides.
- **Pre-created topics**: the number of subscribers is configurable, so topics must follow registration.

## N8. Delivery: the notification table as outbox, retries, and parking

**Decision**: the `notification` row is the notification's outbox entry and delivery record at once (FR-016, Principle VII). `NotificationDispatcher` (business layer, Kafka-free behind the existing `RecordSender` port) publishes it:
- **Immediate path and sweep**, as in 006 D5: newly selected IDs go to an in-memory queue after the selection commit, and a sweep every 1 s selects `PENDING` rows whose `next_attempt_at` is due, so crashes, outages, and retries need no memory.
- **Per-subscriber lanes**: each subscriber has its own in-memory lane with at most 64 sends in flight. A stalled or failing channel fills only its own lane, and its further due rows wait in the database. The dispatcher thread never blocks on a send (sends are asynchronous, bounded by the producer's `max.block.ms`).
- **Delivered**: the broker confirmed the publish with `acks=all` (FR-016). Confirmations are batched into one `UPDATE … SET state = 'DELIVERED', delivered_at = ?` per 20 ms or 200 IDs, off the critical path.
- **Failure and retry**: a failed or timed-out send increments `attempt_count`, records `last_error`, and schedules `next_attempt_at` with backoff 200 ms × 2ⁿ, capped at 5 s. Producer `delivery.timeout.ms` is 10 s, so a stalled send fails within 10 s.
- **Charged failures and parking**: a failure is **charged** to the channel only if some other subscriber's channel had a confirmed send after this attempt started; otherwise it is an uncharged, broker-wide failure. After 8 charged failures (about 21 s of backoff), the row becomes `PARKED` with an alarm (FR-017). A broker outage therefore never parks notifications: they retry until the broker returns, and a stall alarm fires after 60 s without any confirmed send.
- **Operator retry**: `POST /notifications/ops/parked/retry` (all, or one subscriber) sets parked rows back to `PENDING` with charged failures reset, keeping their identity (FR-017).
- **Redelivery**: a crash between a confirmed send and the marking batch re-sends the row after restart with the same notification identity. The subscriber records it as a redelivery (FR-015, FR-021).

**Rationale**: reusing the outbox pattern keeps every publish backed by a durable row. Charging failures only when other channels succeed separates a faulty channel from a faulty broker without any extra infrastructure, so a broker outage never turns into parked notifications.

**Alternatives considered**:
- **Reusing `OutboxDispatcher` unchanged**: it retries forever with no per-row state, no parking, and no per-subscriber isolation.
- **Parking after N attempts regardless of cause**: a 30-second broker restart would park every in-flight notification.
- **A dead-letter topic for parked notifications**: parked state must be queryable and retryable, and a table already is (006 D6).

## N9. Delivered vs. received, and latency measurement

**Decision**:
- **Delivered** = the channel acknowledged a durable publish; recorded by Notification Service as `delivered_at`.
- **Received** = the subscriber test application wrote the receipt record; recorded as `received_at_ms` in that record.
- **End-to-end latency** = `received_at_ms` of the first receipt of each notification − the sale's `completed_at_ms` from `SaleCompleted`. Both timestamps come from processes on the same machine, using `System.currentTimeMillis()` and the same wall clock. The notification report also gives completion→delivered latency, from Notification's `delivered_at`, to separate service time from subscriber time.
- **Rates**: sales per second and notifications delivered and received per second over the active window, from the first to the last completed sale, plus totals over the whole run up to the end of the drain.

**Rationale**: the clarification session fixed these definitions. One machine and one clock make cross-process latency meaningful, and no correctness decision uses these timestamps (spec Assumptions).

**Alternatives considered**: latency to delivery only, which would hide the channel and the subscriber; a clock-synchronization protocol, which is unnecessary on one host.

## N10. Subscriber test application

**Decision**: a separate Java 21 application, `subscriber-client/`, built with `javac` like `load-client/`, against `server/out/contracts.jar` and `server/lib/messaging/*`. It never imports server code beyond the generated event classes. Its main classes are:
- **`SubscriberMain`**: `--subscribers=N` (default 100), `--seed=S` (required for evidence runs; when omitted, a seed is generated and recorded), `--skus-per-subscriber=K` (default 20), `--sku-distribution=uniform|popularity` (default `uniform`), `--id-prefix=sub`, `--base-url`, `--bootstrap`, `--out=DIR`, `--resume`.
  1. Fetches the catalog with the public `GET /items`.
  2. Derives each subscriber's identity (`sub-0001` … `sub-N`) and SKUs. Subscriber *i* uses its own generator, `SplittableRandom(mix64(seed + i))`, so subscriber *i* gets the same SKUs at every subscriber count (the sets are nested across counts).
  3. Writes `snapshot.json`, then registers the subscribers through the gateway with 8 parallel requests, retrying ambiguous failures, which is safe because registration is idempotent.
  4. Writes `ready.json`, holding the snapshot's subscription version, only after every registration is confirmed and every channel topic exists.
- **Subscriber threads**: one thread per subscriber. Each owns one `KafkaConsumer` with manual assignment of its channel's partition 0 and no consumer group. It appends one JSON line per received record to `receipts/<subscriberId>.jsonl` and flushes after each poll. On `--resume`, it seeks to one past the highest channel offset already recorded, so nothing is lost and at most the unflushed tail is received again and recorded as a redelivery.
- **Status**: every second it writes `status.json` with per-subscriber received counts and channel lag (end offset − position), which the harness reads for drain detection.
- **`ChangeScenarioMain`**: drives the subscription-change scenario (N11, quickstart scenario E).
- **`ReconcileMain`**: the reconciler (N11).

`popularity` draws SKUs weighted by the Load Client's Zipf weights. It is available to raise notification volume, but evidence runs use `uniform`. With uniform draws, K = 20 gives about 100 × 8 × 20/2,000 ≈ 8 notifications per sale at 100 subscribers, given about 8 distinct SKUs per basket: roughly 35,000 notifications in a default run and 85,000 in a stress run.

**Rationale**: separate from every service (FR-019); reproducible from one recorded seed (FR-020); durable, restartable receipts (FR-021). Nested assignments make runs at different subscriber counts directly comparable.

**Alternatives considered**: extending the Load Client, which is forbidden; one consumer group shared by all subscriber threads, which would not give each subscriber its own channel; Zipf-weighted assignment by default, which concentrates load on a few subscribers and skews per-subscriber latency.

## N11. Reconciliation

**Decision**: `db/export-notification-evidence.ps1` exports each source with its owner's credentials, as `validate-invariants.ps1` does in 006 (FR-024):
- **Transaction** (Transaction credentials): completed transactions with completion time, and per-SKU quantities aggregated from `accepted_scan` of COMPLETED transactions. These are independent of the `SaleCompleted` event. It also exports the `SaleCompleted` outbox rows, for SC-001.
- **Notification** (Notification credentials): `sale_selection`, `notification`, `subscription_change`, `subscriber`, `subscription`, and `dead_letter_event`.
- **Subscriber test application**: `snapshot.json`, `ready.json`, and `receipts/*.jsonl`.

`ReconcileMain --run-dir=DIR --mode=fixed|history` then:
1. Checks SC-001: exactly one `SaleCompleted` outbox row per COMPLETED transaction, with matching lines.
2. Computes the owed set. In `fixed` mode, owed = completed sales × the snapshot. It first verifies that no `subscription_change` has a version above `ready.json`'s version (FR-023) and that every selection's version equals it. In `history` mode, it verifies each selection against the history replayed to its version, and checks causality (N12, scenario E) before using the recorded selections as the applicable snapshot (FR-027).
3. Compares the owed set with selections, delivery records, and first receipts, and reports:
   - missing notifications, split into not delivered and delivered but not received;
   - unexpected notifications;
   - duplicate effective notifications, meaning more than one notification identity or delivery row for one (sale, SKU, subscriber), or one identity recorded with two different keys;
   - quantity mismatches;
   - selection mismatches;
   - redeliveries, parked notifications, and parked events.

   It writes `reconciliation.json` and exits non-zero on any failure (FR-026).
4. Computes latency percentiles and rates from the receipts (N9) for the notification report.

**Drain** (FR-025), sampled every 2 s by the harness: no unpublished `SaleCompleted` outbox row, `notification-sale-completed` lag of 0, no `PENDING` notification, and zero channel lag in `status.json`, holding for two consecutive samples. The bound is 120 s after the Load Client ends (configurable); on timeout the reconciliation still runs, and it fails while listing what is outstanding.

**Rationale**: owners export their own data, so no service reads another's (Principle II). Deriving sales from `accepted_scan` makes the check independent of the event under test. A Java reconciler handles about 100,000 rows quickly and shares the decoders with the subscriber application.

**Alternatives considered**: reconciling from the broker's topics, which is not independent of delivery; a PowerShell reconciler, which is too slow and too hard to test at this volume.

## N12. Subscription-change scenario and causality check

**Decision**: `ChangeScenarioMain` runs against a baseline with a small subscriber set and its own sales, made through the public routes with `Start`, `Scan`, and `Complete` (it is a test client, not the Load Client):
1. **Confirmed-before cases**: register, add, remove, cancel, and re-register. After each confirmation, it completes a sale of the affected SKU and records the pair (confirmed version, transaction ID).
2. **Concurrent cases**: a burst of changes while the Load Client runs.
3. **Failed changes**: the test-only fault `notification.subscription.beforeCommit` throws inside the change transaction, so it rolls back; the driver records the attempted change.
4. **Rejected requests**: requests that FR-006 rejects.

Verification (`ReconcileMain --mode=history`) requires that each sale's selection version is at least the version confirmed before its Complete was sent, which needs no clock (rule 6). Each selection must equal the history replayed to its version. No failed change may appear in the history or in any selection.

**Rationale**: versions returned at commit give a clock-free causality test, and replaying the history checks rule 3 exactly.

**Alternatives considered**: comparing timestamps across services, which correctness must not depend on.

## N13. Fault injection

**Decision**: extend the 006 `FaultHooks` points (system property `selfcheckout.faults`). Because the property is re-read on every hit, Notification Service exposes `POST /notifications/ops/faults` to set or clear it at runtime, enabled only when `NOTIFICATION_TEST_FAULTS=1`, and otherwise answering 404. New points:
- `transaction.sale.beforeT3Commit` and `transaction.sale.afterT3Commit`, for a crash before or after the completion with its event;
- `notification.selection.beforeCommit` and `notification.selection.afterCommit` (before the offset commit);
- `notification.delivery.afterSend` (after confirmation, before marking);
- `notification.subscription.beforeCommit`;
- `notification.channel.reject:<subscriberId>`, where every send to that channel fails at once;
- `notification.channel.stall:<subscriberId>`, where every send to that channel is held for 10 s and then fails as a timeout.

The channel faults act inside Notification's `RecordSender` wrapper, before the record reaches the producer, so the broker and every other channel stay healthy (the clarified SC-006). Stopping a subscriber thread is never used as the isolation fault.

**Rationale**: the hooks already exist, are proven in 006, and keep faults out of production paths unless enabled.

**Alternatives considered**: broker-level quotas or ACLs to break one topic, which affect the shared producer and are fragile on a single Windows node.

## N14. Operations, baseline, and resources

**Decision**:
- **Port and gateway**: Notification HTTP on 8084. `deploy/nginx.conf` gains upstream `notification_service` and `location ^~ /notifications/`. The existing locations are unchanged.
- **Database**: schema `cs6510_notification`, user `cs6510_notification_svc` with password from `NOTIFICATION_DB_PASSWORD` (a new user-level environment variable, never printed), created by `db/bootstrap.sql` and `db/bootstrap.ps1`, with grants limited to its schema.
- **Pool**: `NOTIFICATION_DB_POOL_SIZE` 12, `NOTIFICATION_DB_POOL_MAX_SIZE` 20. The connection budget rises from 122 to 142, still below MySQL's default `max_connections` of 151.
- **Baseline** (FR-036): `db/reset-baseline.ps1` refuses to run while 8081–8084 answer, resets the four schemas (Notification: empty), replaces the broker log directory (removing channel topics), recreates the five fixed topics, and clears the run directory of the subscriber test application.
- **Scripts**: `server/build.sh notification`, `server/run-notification.sh`, and `deploy/kafka/` port lists extended to 8084.
- **Status**: `NotificationStatusQueries` and the status line report the subscription version, index version, active subscribers, selected sales, notifications by state, charged and uncharged failures, parked notifications and events, alarms, and consumer lag (FR-030).

**Rationale**: these mirror the 006 operational contract, so the new service is operated the same way (Principle VIII).

## N15. Performance model and evidence

**Decision**:
- **Cost on the checkout path**: one more INSERT in T3, a `MEDIUMBLOB` of about 200 bytes, plus one dispatcher send and a batched mark. The design estimate is a ≤ 3% drop in `totalTransactions`, checked at CP1. Selection and delivery run off the checkout path.
- **Notification path per sale**: one selection transaction, with one locking read, one selection INSERT, about one notification INSERT, and one commit. Per notification: one asynchronous send plus a share of a batched mark. At stress rates this is about 90 selection commits per second plus about 50 marking commits per second.
- **Checkpoints**:
  - **CP0**: 006 at `4a91686` on this machine, default and stress.
  - **CP1**: `SaleCompleted` recorded and published, with no Notification Service running (the cost on the checkout path).
  - **CP2**: the full system with 100 subscribers.
  - **CP3**: optional A/B of consumer threads (4 vs 8), lane in-flight cap, and marking batch size.
  - **CP4**: the official runs.

  A drop of more than 5% between checkpoints is investigated, as in 006.
- **Evidence runs**:
  - **Scaling**: subscriber counts 10, 25, 50, and 100, each with the default Load Client (10 stations, 60 s), seed `20261111`, K = 20, uniform.
  - **Standard reports**: default and stress with 100 subscribers.
  - **Isolation (SC-006)**: 3 healthy, 3 reject, and 3 stall runs at default with 100 subscribers and the same seed, run interleaved.

  Each run commits the Load Client's JSON report unmodified, plus `notification-report.json`, a rendered `notification-report.md`, and `reconciliation.json` under `load-client/reports/notification-service/` ([contracts/deployment-and-evidence.md](contracts/deployment-and-evidence.md)).

**Rationale**: four counts exceed the required three and include 100. The default workload keeps the scaling runs short and comparable. Interleaving the isolation runs reduces drift.

## N16. Testing strategy

**Decision**: follow the 006 layering:
- **Unit tests**: index and catch-up, change validation and idempotency, selection computation, retry and charging rules, the reconciler with injected mismatches (SC-003), and seed determinism (SC-013).
- **Database tests**: the change transaction, the selection transaction with the shared lock, duplicate sales, the failed-change rollback, isolation of the notification credentials.
- **In-memory bus tests** (`InMemoryEventBus`): the sale-to-receipt flow, duplicate events, the crash matrix at each hook.
- **Live Kafka tests** (`tests/messaging`): `SaleCompleted` compatibility, channel provisioning, reject and stall isolation, a broker restart during delivery, parking and operator retry.
- **Architecture tests**:
  - Transaction does not reference subscriptions or recipients (FR-004);
  - Notification publishes only from notification rows;
  - layer dependencies, boundaries, and composition roots extended to four services;
  - the diagram is consistent with the catalog.
- **Performance tests**: T3's statement budget rises to 3, by design and recorded with its reason; the selection budget is added.
- **End-to-end**: quickstart scenarios A–H, plus feature 006's release suite unchanged (FR-031).

## N17. Deferred options

| Option | Entry criterion |
|---|---|
| Batched selection, with several sales per transaction | Selection commit rate limits drain time at 100 subscribers (sale-consumer lag grows during the stress run) |
| Purging delivered notification rows | Notification table growth affects a run (it does not at about 100,000 rows) |
| Per-subscriber ordering | A requirement appears; the spec currently guarantees none |
| Multiple Notification processes in benchmarks | A throughput need; correctness already holds through the version check |
