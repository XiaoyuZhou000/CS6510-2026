---

description: "Task list for replacing synchronous inter-service calls with Kafka-carried domain events"
---

# Tasks: Event-Based Self-Checkout Microservices with a Message Broker, Broker-Carried Saga, and Event-Driven Replication

**Input**: Design documents from `/specs/006-event-based-architecture/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md) (D1–D17), [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)

**Tests**: Requested by spec FR-064 and FR-065 and plan D14.
- **Test first**: within each story, write the test tasks first and confirm that they fail (or, for adapted tests, that they fail against the new assertions) before starting the implementation tasks.
- **Parity gate**: each story ends with a gate. A story is complete only when every assertion of a removed or replaced test that it owns has a passing equivalent, as recorded in the feature 006 section of `server/tests/COVERAGE-LEDGER.md` (FR-065).

**Organization**: Tasks are grouped by user story.
- **Story order** follows the plan's *Incremental Evolution Path*, not strictly the priority numbers. All stories are required; priority sets delivery order only.
  - US1 (checkout over events) comes first, then US3 (its fault tolerance), so the Saga is complete before anything else moves (CP1).
  - US4 (catalog) comes before US5 (scans), and US2 (scan independent of the broker) comes after US5, because US2's acceptance scenario 3 needs scans delivered through the broker.
  - US6 and US7 verify the external contract and independent operation once every gRPC path is gone. US8 produces the evidence.
- **Checkpoints**: CP0 (feature 005 baseline, T002), CP1 (end of US1, T079), CP2 (end of US5, T131), CP3 (tuning A/B, T163), CP4 (official evidence, T164–T166). See plan.md *Performance Strategy* and research D15.
- **Task IDs** are in execution order. Execute tasks in document order within each phase, subject to the dependencies below.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel: different files, and no dependency on an incomplete task.
- **[Story]**: The user story the task serves (US1–US8).
- Every task names its exact file paths.

## Path Conventions

- **Production code**: `server/<service>/src/<package>/…`. Packages are `api`, the business package (`transaction`, `inventory`, `analytics`), `database`, and `messaging` (replacing `rpc`). Shared code goes in `server/common/src/{api,database,metrics,events,messaging,faults,time}/`; the Java package is the directory name, as today (`package events;`). `events` holds the Kafka-free event framework (ports, codec, `OutboxDispatcher`, `ConsumerLoop`), which business code may import; `messaging` holds only Kafka adapters, which business code never imports. Generated event classes go in `server/contracts/` (`package selfcheckout.events.v1`).
- **Tests**: `server/tests/{unit,architecture,database,contract,integration,messaging,performance,support}/…`, compiled by `server/build-tests.sh` and run by `server/run-tests.sh`.
- **Deployment**: `deploy/kafka/` for the broker, `deploy/nginx.conf` unchanged, `db/` for schemas and baseline tooling.
- **Evidence**: `load-client/reports/event-based-architecture/` and `docs/architecture/`.

## Implementation Rules

These rules bind every task below.

1. **Carried over from feature 005** (`specs/005-microservices-architecture/tasks.md`, *Implementation Rules*): striped locks, process-local components that are never authoritative, the designated in-memory state owners (now also the dispatch queue, marking batch, and catalog version buffer, data-model *In-memory projections*), and benchmark claims based only on client-observed `totalTransactions`.
2. **No publish without an outbox row** (FR-010): only `OutboxDispatcher` and `KafkaScanEventSink` may call a `RecordSender`. Business code writes an outbox row inside its own local transaction and signals the dispatcher after the commit.
3. **Acknowledge after commit** (FR-011): `ConsumerLoop` commits an offset only after the handler's local transaction committed, or after a `dead_letter_event` row committed.
4. **Kafka types stay in adapters**: `org.apache.kafka.*` imports are allowed only in `server/common/src/messaging/` and `server/<service>/src/messaging/`. Business handlers depend on the `EventHandler`, `OutboxSignal`, and `OutboxStore` ports in package `events` (`server/common/src/events/`) and on generated `selfcheckout.events.v1` classes. The business layer never imports `messaging`; only the `*Main` composition roots wire `messaging` adapters to `events` ports.
5. **Shared JDBC adapters**: `outbox_event` and `dead_letter_event` have the same shape in every schema, so their JDBC adapters live once in `server/common/src/database/` and are instantiated with the owning service's own pool. This refines plan.md's per-service listing: `build-tests.sh` puts all three service jars on one classpath, where identically named per-service classes would shadow each other.
6. **Shared machine** (feature 005 memory): never build or run tests while a benchmark runs; run `server/benchmark-compare.ps1` with `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR`; retry a service start that fails with a `BindException`; save scratch `.ps1` files that contain the repository path as UTF-8 with BOM.
7. **Credentials**: never print or pass database passwords as arguments; use the per-service `*_DB_PASSWORD` user environment variables and `DB_USER`/`DB_PASSWORD` for admin work.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Commit the governing constitution, record the baseline, add the broker, and add event contracts and the Kafka client to the build alongside the existing gRPC artifacts.

- [X] T001 Commit constitution v6.0.0 in `.specify/memory/constitution.md`:
  - Show the user the working-tree diff of the principles and governance text (excluding the Sync Impact Report) and get explicit approval of that exact text before committing (feature 005 Implementation Rule 5).
  - Remove the leading Sync Impact Report comment block, as Governance requires, leaving every principle unchanged.
  - Commit it alone, or together with `specs/006-event-based-architecture/` and `Event Based Architecture Requirement.txt`, before any code change of this feature.
- [X] T002 Run CP0 on the unmodified feature 005 implementation at commit `8891c57`, before any code change, and record it in a new `load-client/reports/event-based-architecture/checkpoints.md`:
  - Build with `server/build.sh` and `load-client/build.sh`. Run `server/benchmark-microservices.ps1` with the shared-machine settings, twice with `-Workload default` (labels `cp0-default-1`, `cp0-default-2`) and once with `-Workload stress` (label `cp0-stress-1`), writing output under `load-client/reports/event-based-architecture/cp0/`.
  - Create the results table with the columns of feature 005's `load-client/reports/microservices-architecture/checkpoints.md`: checkpoint, label, workload, `totalTransactions`, change against the CP0 median of the same workload (%), Start/Scan/Complete p95/p99, Complete error count, background completions, `innodb_flush_log_at_trx_commit`, `sync_binlog`, and a link to the harness output. Add the columns `parked events` and `final consumer lag` (blank for CP0).
  - These runs are the attribution reference for CP1–CP3. The CP0-versus-CP4 comparison uses interleaved runs (T166).
- [X] T003 Extend `server/contracts/pom.xml` (D2), keeping every gRPC dependency and plugin goal for now:
  - Add compile-scope `org.apache.kafka:kafka-clients:4.1.0` and runtime-scope `org.slf4j:slf4j-simple:2.0.16`, and pin `com.google.protobuf:protobuf-java` explicitly at `3.25.9`.
  - Add a second `maven-dependency-plugin` `copy-dependencies` execution (includeScope runtime) to `${project.basedir}/../lib/messaging` at `package`, next to the existing `lib/grpc` execution.
  - Add `server/contracts/src/main/resources/simplelogger.properties` with `org.slf4j.simpleLogger.defaultLogLevel=warn`, packaged into `contracts.jar`.
- [X] T004 Revise `server/tests/architecture/MicroserviceBoundaryTest.java` and `server/tests/architecture/LayerDependencyTest.java` so the Kafka client can land (FR-065):
  - In `MicroserviceBoundaryTest`, remove `kafka` from the forbidden-dependency pattern and keep `redis|jedis|lettuce|rabbit|activemq` forbidden (one broker product, FR-007). Add a source check that `org.apache.kafka` imports appear only under `server/common/src/messaging/` and `server/<service>/src/messaging/`. Keep the durability-setting checks unchanged.
  - In `LayerDependencyTest`, add `messaging` as an adapter layer with the same rules as `rpc` (may depend on the business layer, `events`, and the rest of `common`; the business layer never imports `messaging`), keeping `rpc` until T148.
  - Also add `events` as a shared package that the business, `database`, and `messaging` layers may import and that itself imports no `messaging`, `org.apache.kafka`, or business package, and extend the business-free check of `common.jar` and `common/src` to `events`, `messaging`, `faults`, and `time`.
  - Record both edits in the 006 ledger section (T043) with requirement FR-065: the broker ban becomes a synchronous-call ban in T146.
- [X] T005 [P] Copy the four design schemas verbatim from `specs/006-event-based-architecture/contracts/proto/{envelope,catalog_events,checkout_events,scan_events}.proto` to `server/contracts/src/main/proto/selfcheckout/events/v1/`, making the server copies authoritative (Constitution III). Replace the body of each spec copy with a one-line comment pointing to its server path, as feature 005 did.
- [X] T006 Update `server/build.sh` (D2):
  - `build_contracts` also verifies that `lib/messaging/kafka-clients-4.1.0.jar` exists, and still verifies `out/contracts.jar` and `lib/grpc/`. Keep `MAVEN_OFFLINE=1` support.
  - Compile `common` and each service with `lib/messaging/*` added to the classpath after `lib/grpc/*`. Quote the wildcard and keep the Windows `;` separator.
  - Run `./build.sh` and `./build.sh inventory`, and confirm that the jars are produced.
- [X] T007 [P] Update `server/run-transaction.sh`, `server/run-inventory.sh`, and `server/run-analytics.sh`: fail clearly if `lib/messaging/` is missing, and add `lib/messaging/*` to the launch classpath. Keep the optional port argument.
- [X] T008 [P] Update `server/build-tests.sh` and `server/run-tests.sh`:
  - Add `lib/messaging/*` to the javac and java classpaths.
  - Pass `KAFKA_TEST_BOOTSTRAP` (default `127.0.0.1:19092`), `KAFKA_TEST_BROKER_CONTROL` (originally `KAFKA_TEST_CONTAINER`), `KAFKA_HOME`, and `NGINX_BIN` through as `-D` properties.
  - When `STRICT_LIVE_GATES=1`, skipped tests in the `messaging.`, `contract.`, `integration.`, and `performance.` packages fail the run, extending the existing `SERVER_BASE_URL` rule (D14).
  - Accept `PARITY_GATE=006:<gate>` and pass it as `-Dparity.gate=006:<gate>` (T043).
  - Report the classes listed in `RESTORATION_DIRECT_CLASSES` (initially `messaging.RestorationEventDirectTest`) in a separate "Direct restoration (SC-016)" section of the summary, excluded from the end-to-end Saga totals (T086).
- [X] T009 [P] Add `server/lib/messaging/` to `.gitignore`, next to the existing gRPC entries.
- [X] T010 [P] Create `deploy/kafka/server.properties` (D1 as amended 2026-10-29: Docker cannot run on the benchmark machine, so the broker runs natively; [broker-and-deployment.md](contracts/broker-and-deployment.md#broker-install-start-stop-reset)): `process.roles=broker,controller`, `node.id=1`, a single-voter controller quorum on `127.0.0.1:19093`, `PLAINTEXT` and `CONTROLLER` listeners on `127.0.0.1:19092` and `127.0.0.1:19093`, replication factors of 1, `auto.create.topics.enable=false`, `unclean.leader.election.enable=false`, and the Windows file-lock rules (segments never roll, no retention, `delete.topic.enable=false`). Create `deploy/kafka/start-broker.ps1` (write the effective configuration with `log.dirs` under `KAFKA_DATA_DIR` and `log.flush.interval.messages=1` only when `KAFKA_FLUSH_EVERY_MESSAGE=true`, format on first use with a fixed cluster ID, start `kafka.Kafka` as a hidden JVM from `KAFKA_HOME`, wait for it, and run `create-topics.ps1`), `deploy/kafka/stop-broker.ps1` (a forced stop), and the shared helper `deploy/kafka/kafka-tools.ps1`, which runs the Kafka tools as Java main classes because the Windows `.bat` wrappers fail on long classpaths. *Originally a Docker Compose file; replaced.*
- [X] T011 Create `deploy/kafka/create-topics.ps1` (depends on T010): wait for the broker, then create idempotently through the Kafka topic tool the four topics of the [event catalog](contracts/event-catalog.md#topics), honoring an optional `-TopicPrefix`, each also pinning `segment.bytes=1073741824` and `segment.ms=9223372036854775807`:
  - `selfcheckout.catalog.v1`: 1 partition, `cleanup.policy=compact`, `retention.ms=-1`.
  - `selfcheckout.checkout.v1` and `selfcheckout.checkout-outcome.v1`: 16 partitions, `cleanup.policy=delete`, `retention.ms=604800000`.
  - `selfcheckout.scan-accepted.v1`: 1 partition, `cleanup.policy=delete`, `retention.ms=604800000`.
  - Verify each topic's partition count and configuration afterwards, and exit nonzero on a mismatch. Save the script as UTF-8 with BOM.
- [X] T012 [P] Create `deploy/kafka/status.ps1`: print broker reachability, each topic's partitions and end offsets, and per-group lag for `inventory-checkout`, `transaction-checkout-outcome`, and `analytics-scan-ingest` from `kafka-consumer-groups --describe --all-groups`, in three tool runs. A group that does not exist yet is reported as `absent`, not as an error. Support `-Json` for the harness (T041).
- [X] T013 [P] Create `deploy/kafka/reset-topics.ps1` (D13): refuse to run while any of ports 8081–8083 answers; because a topic cannot be deleted on Windows, stop the broker, delete `KAFKA_DATA_DIR\kraft-logs`, and start it again, which reformats and recreates the four topics through `create-topics.ps1`; then verify that every end offset is 0 and that none of the three groups exists.
- [X] T014 Create `deploy/kafka/README.md` (FR-056): installing the Kafka 4.1.0 binary release and `KAFKA_HOME`, start, stop (always forced on Windows), restart, status, topic creation and reset, full removal, the listeners and the state directory, the configuration above, the Windows constraints (no topic deletion, never-rolling segments, the `.bat` limitation), `KAFKA_FLUSH_EVERY_MESSAGE` and its residual file-cache risk on host loss (D1), and the rejected alternatives summarized from D1.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The shared messaging framework (envelope, outbox dispatcher, consumer loop, parking), the new tables, the in-memory and live test infrastructure, the baseline and benchmark tooling, and the 006 coverage ledger. Every user story depends on this phase.

**⚠️ CRITICAL**: No user-story task starts until this phase is complete.

### Tests for the foundation ⚠️ write first; they must fail before implementation

- [X] T015 [P] Create `server/tests/unit/common/EnvelopeCodecTest.java` (D3, [event-catalog.md](contracts/event-catalog.md#envelope)): round-trips every one of the ten event types; rejects as permanent failures a missing `event_id`, `event_type`, `routing_key`, or `producer`, a `producer` that is not the type's owner, an unsupported `schema_version`, an unknown `event_type`, an undecodable payload, and payload identity fields that do not match `routing_key` (for example `CheckoutRequested.attempt.transaction_id` ≠ key); and asserts that scan event IDs are `scan:<transaction_id>:<scan_sequence>`.
- [X] T016 [P] Create `server/tests/unit/common/OutboxDispatcherTest.java` (D5) against an in-memory `OutboxStore` and a scripted `RecordSender`:
  - Immediate path: `signal(eventIds)` returns without waiting for the sender; events are sent in signal order.
  - Confirmation: `published_at` is marked only after the send future succeeds, in batches of at most 200 IDs or 20 ms (`OUTBOX_MARK_BATCH_MS`), one store call per batch.
  - Sweep: every `OUTBOX_SWEEP_MS` (1,000 ms) it selects at most 500 rows with `published_at IS NULL AND created_at < now − 1 s`, in `outbox_seq` order, re-sends them with backoff from 100 ms to 5 s, and increments `send_count`.
  - A failed send leaves the row unpublished; a dispatcher restart re-sends it; a re-send reuses the stored `event_id` and envelope bytes.
  - `resend(eventId)` re-sends one recorded row by `event_id`.
  - Each row goes to the sender registered for its topic; a dispatcher built with a row topic that has no registered sender fails at construction.
  - With the immediate path disabled (test flag), the sweep alone publishes every row (plan *Complexity Tracking*).
- [X] T017 [P] Create `server/tests/unit/common/ConsumerLoopTest.java` (D6) against an in-memory `RecordStream`:
  - The offset is committed only after the handler returned success, and never when it threw.
  - A transient failure pauses only that partition and retries the same record with backoff from 100 ms to 5 s, indefinitely, committing nothing; other partitions keep flowing.
  - A permanent failure is retried `CONSUMER_MAX_ATTEMPTS` (5) times 200 ms apart, then one `dead_letter_event` row is written and the offset committed; a parked `event_id` that arrives again increments `park_count`.
  - An undecodable record is parked with a null `event_id` and reason `UNDECODABLE`.
  - Classification: a `database.StoreFailure` (database unavailable, contention exhausted, or commit outcome unknown) is transient; any other exception that is neither `TransientEventFailure` nor `PermanentEventFailure` is permanent and parked with reason `HANDLER_ERROR` and its class and message in `detail`, so an unexpected bug never blocks a partition indefinitely (FR-013).
  - A record that stays in transient retry longer than 60 s is logged as stalled (topic, partition, offset, age) once per status interval, and `stalledPartitions()` reports it.
  - On revocation and shutdown, processed offsets are committed with `commitSync`.
  - Manual-assignment mode seeks to a supplied offset (0 for the catalog, a stored offset for Analytics).
- [X] T018 [P] Create `server/tests/messaging/EventSchemaCompatibilityTest.java` and its committed baseline `server/tests/messaging/baseline/event-descriptor-summary.txt` (FR-014, D3), reusing the method of `server/tests/rpc/ProtoCompatibilityTest.java`: compare the compiled descriptors of `selfcheckout.events.v1` with the baseline and fail on a removed or renumbered field, a changed field type, a removed enum value, or a removed message, while allowing new optional fields with new tag numbers. Also assert that bytes produced by the current classes decode with a parser built only from the baseline descriptors.
- [X] T019 [P] Create `server/tests/database/JdbcOutboxStoreTest.java` against the Transaction and the Inventory schemas: an insert inside a caller's open `JdbcUnitOfWork` becomes visible only after that commit and disappears on rollback; `event_id` is unique; `markPublished(ids)` is one UPDATE per batch; the sweep query uses `(published_at, outbox_seq)` and respects `created_at < now − 1 s` and the 500-row limit; `findByEventId` and `findByAggregateKey` return the stored envelope bytes byte-identically.
- [X] T020 [P] Create `server/tests/database/JdbcDeadLetterStoreTest.java` against all three schemas: a park inserts the row with topic, partition, offset, type, version, reason, detail truncated to 500 characters, and raw bytes; parking the same non-null `event_id` again increments `park_count` and `last_parked_at`; null `event_id` rows never collide; `countParked()` feeds the status line.
- [X] T021 [P] Create `server/tests/messaging/KafkaTransportLiveTest.java` against `KAFKA_TEST_BOOTSTRAP` with a per-run topic prefix (skipped when no broker, failing under `STRICT_LIVE_GATES=1`):
  - A send completes only after `acks=all`, and an idempotent producer keeps per-key order across 1,000 records on a 16-partition topic.
  - With the broker unreachable, a send fails within `max.block.ms` (1 s) and the caller thread never blocks longer.
  - `ConsumerLoop` commits offsets only after handler success: a consumer restarted after a handler failure re-reads the same record.
  - A poison record on one partition is parked within about 1 s while records on another partition keep flowing.
  - Manual assignment replays a compacted topic from offset 0.
- [X] T022 [P] Create `server/tests/unit/common/InMemoryEventBusTest.java`: the bus's partitions, consumer groups, offset commits, and each fault control (duplicate, drop before consume, delay, reorder across partitions, redeliver after a lost acknowledgement, pause as broker down, refuse sends as a broker whose storage is full, and kill a consumer between commit and acknowledgement) behave deterministically under a fixed seed.

### Implementation for the foundation

- [X] T023 Move `server/common/src/rpc/FaultHooks.java` to `server/common/src/faults/FaultHooks.java` (`package faults;`) and `server/common/src/rpc/ResponseBudget.java` to `server/common/src/time/ResponseBudget.java` (`package time;`), without behavior changes, and update every import: `transaction/src/transaction/{CheckoutOrchestrator,ScanEventRelay,TransactionService}.java`, `inventory/src/inventory/CheckoutStockService.java`, `analytics/src/analytics/ScanIngestionService.java`, `analytics/src/database/JdbcScanIngestStore.java`, and the tests that use them (`unit/common/FaultHooksTest`, `unit/common/ResponseBudgetTest`, `support/FaultInjection`, `database/JdbcScanIngestStoreTest`, `performance/{HeldConnectionDuringWaitTest,MultiInstanceTransactionTest,TransactionCacheCorrectnessTest}`, `unit/transaction/TransactionServiceTest`, `architecture/MicroserviceBoundaryTest`). Record both test files as `ADAPTED` in the ledger.
- [X] T024 [P] Create `server/common/src/events/TopicNames.java` and `server/common/src/events/EventTypes.java`:
  - `TopicNames`: the four topic names, prefixed by `KAFKA_TOPIC_PREFIX` (default empty), and the group names `inventory-checkout`, `transaction-checkout-outcome`, `analytics-scan-ingest`.
  - `EventTypes`: for each of the ten types in the [event catalog](contracts/event-catalog.md#event-types), its topic, owning producer (`transaction` or `inventory`), supported schema versions (`{1}`), and payload parser.
- [X] T025 Create `server/common/src/events/EnvelopeCodec.java` and `server/common/src/events/EventDecodeException.java` (depends on T024): `encode(eventId, type, routingKey, correlationId, producer, occurredAtMs, payload)` builds a serialized `EventEnvelope` with `schema_version = 1`; `decode(bytes, recordKey)` performs every envelope check of T015 and throws `EventDecodeException` carrying a `dead_letter_event.reason` value (`UNDECODABLE`, `UNKNOWN_TYPE`, `UNSUPPORTED_VERSION`, `INVALID_PAYLOAD`, or `PAYLOAD_MISMATCH`).
- [X] T026 [P] Create the ports in `server/common/src/events/` (`package events;`, no Kafka imports):
  - `OutboxStore` (`insert(uow, OutboxRow)`, `markPublished(ids, at)`, `selectUnpublished(olderThan, limit)`, `findByEventId`, `findByAggregateKey`, `incrementSendCount`, `countUnpublished`) and the record `OutboxRow(eventId, topic, routingKey, eventType, aggregateKey, envelope, createdAt)`.
  - `DeadLetterStore` (`park(ParkedRecord)`, `countParked`).
  - `OutboxSignal` (`signal(List<String> eventIds)` and `resend(String eventId)`, so business code never references `OutboxDispatcher`), `EventHandler` (`handle(DecodedEvent)`), and `DecodedEvent` (envelope fields, decoded payload, topic, partition, offset).
  - `RecordSender` (`send(topic, key, bytes)` returning `CompletableFuture<Void>`) and `RecordStream` (poll, pause, resume, seek, commit, assignment callbacks).
  - `TransientEventFailure` and `PermanentEventFailure` (with a `dead_letter_event.reason`).
- [X] T027 Create the Kafka adapters in `server/common/src/messaging/` (`package messaging;`, the only common package that imports `org.apache.kafka`; depends on T026): `KafkaClients` builds producers with `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `delivery.timeout.ms=30000`, `max.block.ms=1000`, and a per-producer `linger.ms` (0 for checkout and outcome events, `SCAN_PRODUCER_LINGER_MS` = 5 for scans and catalog), and consumers with `enable.auto.commit=false`, `auto.offset.reset=earliest`, `isolation.level=read_uncommitted`, `fetch.min.bytes=1`, `fetch.max.wait.ms=50`, `max.poll.records=500`, all from `KAFKA_BOOTSTRAP` (default `127.0.0.1:19092`). `KafkaRecordSender` and `KafkaRecordStream` implement `RecordSender` and `RecordStream`.
- [X] T028 Create `server/common/src/events/OutboxDispatcher.java` implementing `OutboxSignal` (D5, depends on T026; makes T016 pass):
  - One dispatcher thread drains an in-memory queue in signal order and sends each row's stored envelope with its `routing_key` through the `RecordSender` registered for the row's topic. The topic-to-sender map is supplied at construction, so one dispatcher can send catalog records with a batching producer and outcome records with an immediate one; a row for an unregistered topic is a startup error, never a silent drop.
  - Confirmed IDs join a marking batch flushed every `OUTBOX_MARK_BATCH_MS` (20) or at 200 IDs with one `markPublished`.
  - The sweep runs every `OUTBOX_SWEEP_MS` (1,000), selects at most 500 unpublished rows older than 1 s in `outbox_seq` order, and re-sends them with backoff from 100 ms to 5 s.
  - `resend(eventId)` re-sends a recorded row by `event_id` (D5 point 4) and increments `send_count`.
  - Fault hooks `outbox.beforeSend`, `outbox.afterConfirm`, and `outbox.beforeMark` ([checkout-saga.md](contracts/checkout-saga.md#test-only-fault-hooks)); `PERF_METRICS` timers `outbox.commitToSend`, `outbox.sendToConfirm`, and `outbox.markBatchSize`; ordered `stop()` with a final mark.
- [X] T029 Create `server/common/src/events/ConsumerLoop.java` and `server/common/src/events/ParkingPolicy.java` (D6, depends on T025–T027; makes T017 pass): `N` threads with one `RecordStream` each in group mode, or one thread in manual-assignment mode with a seek supplier; per-record decode through `EnvelopeCodec`, then `EventHandler.handle`, then offset commit (`commitAsync`, `commitSync` on revocation and shutdown); transient handling, classification, stall logging, and parking exactly as T017; fault hook `consumer.afterHandlerCommit` between handler return and offset commit; `PERF_METRICS` timers `consumer.<group>.lag` (record timestamp to handler start) and `consumer.<group>.handler`, counter `consumer.<group>.parked`; a `lag()` gauge (end offset minus committed offset per assigned partition) for the status lines.
- [X] T030 [P] Add to `db/transaction/schema.sql` the new tables and the rename (data-model *Transaction schema*), and update `db/transaction/validate.sql`:
  - `outbox_event`: `outbox_seq` BIGINT auto-increment PK; `event_id` CHAR(36) UNIQUE; `topic` VARCHAR(64); `routing_key` VARCHAR(64); `event_type` VARCHAR(48); `aggregate_key` VARCHAR(64) null ("`<transaction_id>:<attempt_number>` for checkout events … null for catalog events"); `envelope` MEDIUMBLOB; `created_at` TIMESTAMP(3); `published_at` TIMESTAMP(3) null; `send_count` INT default 0; indexes `(published_at, outbox_seq)` and `(aggregate_key)`.
  - `dead_letter_event`: `dead_letter_id` BIGINT auto-increment PK; `event_id` VARCHAR(80) null, UNIQUE ("Null only when the envelope could not be decoded"); `topic` VARCHAR(64), `partition_no` INT, `record_offset` BIGINT; `event_type` VARCHAR(48) null, `schema_version` INT null; `reason` VARCHAR(64) restricted to `UNDECODABLE`, `UNKNOWN_TYPE`, `UNSUPPORTED_VERSION`, `INVALID_PAYLOAD`, `PAYLOAD_MISMATCH`, `UNKNOWN_ATTEMPT`, `INTEGRITY`, `HANDLER_ERROR` (CHECK constraint); `detail` VARCHAR(500); `record_bytes` MEDIUMBLOB; `first_parked_at` TIMESTAMP(3), `last_parked_at` TIMESTAMP(3), `park_count` INT.
  - `accepted_scan.delivered_at` renamed `published_at`, and index `(delivered_at, scan_id)` renamed `(published_at, scan_id)`.
  - Grants stay limited to the Transaction user. `validate.sql` reports unpublished outbox rows, unpublished scans, and parked rows.
- [X] T031 [P] Add `outbox_event` and `dead_letter_event` with exactly the shape of T030 to `db/inventory/schema.sql`, and unpublished and parked counts to `db/inventory/validate.sql`. (The new `checkout_operation` and `catalog_meta` columns are added by T061 and T105, when their writers change.)
- [X] T032 [P] Add to `db/analytics/schema.sql` `dead_letter_event` with the shape of T030 and `consumer_offset` (`topic` VARCHAR(64), `partition_no` INT, PK `(topic, partition_no)`; `next_offset` BIGINT with CHECK `next_offset >= 0`; `updated_at` TIMESTAMP(3)), and parked and offset rows to `db/analytics/validate.sql`.
- [X] T033 Apply the `published_at` rename in code (depends on T030): `server/transaction/src/database/JdbcRelayStore.java`, `server/transaction/src/database/RelayStore.java`, `server/transaction/src/database/TransactionStatusQueries.java` (status field `undelivered_scans` renamed `unpublished_scans`), `db/validate-invariants.ps1`, and the adapted assertions in `server/tests/database/JdbcRelayStoreTest.java` and `server/tests/database/ServiceStatusQueriesTest.java`. No behavior change; the relay still sends through gRPC until T126.
- [X] T034 Create `server/common/src/database/JdbcOutboxStore.java` implementing `OutboxStore` for the caller's own pool (Implementation Rule 5, depends on T026 and T030–T031; makes T019 pass). Rows are only ever updated in `published_at` and `send_count`.
- [X] T035 [P] Create `server/common/src/database/JdbcDeadLetterStore.java` implementing `DeadLetterStore` with `INSERT … ON DUPLICATE KEY UPDATE park_count = park_count + 1, last_parked_at = VALUES(last_parked_at)` (depends on T026 and T030–T032; makes T020 pass).
- [X] T036 [P] Create `server/tests/support/InMemoryEventBus.java` (D14) implementing `RecordSender` and a `RecordStream` factory with partitions by key hash (same partition counts as production), consumer groups, committed offsets, and the fault controls of T022; makes T022 pass.
- [X] T037 [P] Create `server/tests/support/KafkaTestSupport.java` (per-run topic prefix; topic creation through the Kafka `Admin` client with the catalog's partitions and configuration, never deleting a topic, D1; `restartBroker()` and `stopBroker()`/`startBroker()` through `deploy/kafka/stop-broker.ps1` and `start-broker.ps1 -NoTopics`, gated by `KAFKA_TEST_BROKER_CONTROL=1`; and a skip-or-fail helper honoring `STRICT_LIVE_GATES`) and `server/tests/support/PublishRawRecord.java` (a `main` and a helper that publish arbitrary bytes with a key to a topic, for quickstart scenario G).
- [X] T038 [P] Update `server/tests/support/TransactionTestSchema.java`, `server/tests/support/InventoryTestSchema.java`, and `server/tests/support/AnalyticsTestSchema.java` so test schemas include the new tables and their resets empty `outbox_event`, `dead_letter_event`, and `consumer_offset`.
- [X] T039 Create `db/reset-baseline.ps1` (D13, FR-059): refuse to run while ports 8081–8083 answer; run `deploy/kafka/reset-topics.ps1`; run `db/reset-all.ps1` (whose `schema.sql` runs recreate and empty `outbox_event`, `dead_letter_event`, and `consumer_offset`); then print 2,000 products, 2,000 stock rows, 20,000,000 units, and zero transactions, scans, outbox rows, parked events, and topic offsets, failing on any mismatch. Save as UTF-8 with BOM.
- [X] T040 Extend `db/validate-invariants.ps1`: add checks for zero unpublished `outbox_event` rows in Transaction and Inventory, zero unpublished `accepted_scan` rows, zero `dead_letter_event` rows in all three schemas, and, when `-KafkaStatusJson` is supplied, zero lag for every group, where an `absent` group passes only if its topic's end offsets are all 0 (before US5 the scan group does not exist and the scan topic is empty); keep every feature 005 check (stock conservation, no negative stock, positions 1..N without gaps, window correctness).
- [X] T041 Create `server/benchmark-event-based.ps1` from `server/benchmark-microservices.ps1` ([broker-and-deployment.md](contracts/broker-and-deployment.md#benchmark-harness)): check ports 8080–8083 are free and the broker is up with its topics; run `db/reset-baseline.ps1`; start Inventory, Transaction, and Analytics with `PERF_METRICS=true` and the gateway; wait for readiness (initially health only; T112 adds the replica check); run exactly `java -cp out Main` or `java -cp out Main --stations=100 --duration=120`; sample every 2 s feature 005's samples plus `deploy/kafka/status.ps1 -Json` lag, unpublished outbox rows, and parked events; wait for zero pending work; run `db/validate-invariants.ps1`; record MySQL durability settings, the broker version, flush setting, and the JDK running the broker; write `load-client/reports/event-based-architecture/<label>/` with `evidence.json` (the git commit, broker version, flush setting, and every non-default configuration variable, which identify the run because the unchanged client JSON cannot carry them (FR-062); FR-061 counts: durable COMPLETED, background completions, completions without a recorded response, remaining difference from `totalTransactions`, parked events, final lag, `comparable`). Keep `-IdleCpuCores` and `-BlockingProcess`.
- [X] T042 [P] Extend `server/benchmark-compare.ps1` with `-BaselineCommit 8891c57`: the baseline variant runs from a `git worktree` of that commit using its own `benchmark-microservices.ps1`, and the candidate runs `benchmark-event-based.ps1`, interleaved as today; it writes under `load-client/reports/event-based-architecture/compare/`.
- [X] T043 Add a "Feature 006 (gRPC → events)" section to `server/tests/COVERAGE-LEDGER.md` and extend `server/tests/architecture/CoverageParityLedgerTest.java` (FR-065):
  - The section lists every test file at `8891c57` that this feature edits, replaces, or deletes, one row per test method (columns and statuses as in the existing ledger), with gate order `Foundational, US1, US3, US4, US5, US2, US6, US7, US8`. At minimum: `rpc/InventoryCheckoutContractTest` (US1), `rpc/RestoreCheckoutDirectTest` (US3), `rpc/InventoryCatalogContractTest` (US4), `rpc/AnalyticsIngestContractTest` (US5), `rpc/ProtoCompatibilityTest` (US7), `performance/HeldConnectionDuringRpcTest` (US1), `unit/common/RpcStatusClassifierTest` (US7), `support/GrpcTestSupport` and `support/FakeInventoryGateway` (file rows), the tests adapted for compilation by T074, T113, and T127, and every other adapted test named in this task list. Each row names its planned replacement and governing requirement.
  - `CoverageParityLedgerTest` parses both sections. `-Dparity.gate=006:<gate>` enforces the 006 section with its own gate order, and `006:final` requires every 006 row to be non-`PENDING` with a `Verified at` value. The feature 005 section stays enforced as today.
- [X] T044 [P] Update the header of `server/tests/performance/statement-budgets.properties` and `server/tests/performance/StatementBudgetFileTest.java` so an `<key>.evidence` label may name an A/B record in either `load-client/reports/microservices-architecture/checkpoints.md` or `load-client/reports/event-based-architecture/checkpoints.md`. No ceiling changes here.
- [X] T045 Foundational gate: run `./build-tests.sh` and `KAFKA_TEST_BOOTSTRAP=127.0.0.1:19092 KAFKA_TEST_BROKER_CONTROL=1 ./run-tests.sh` from `server/`; T015–T022 pass, every existing test still passes, and `PARITY_GATE=006:Foundational ./run-tests.sh architecture` passes. Fill `Verified at` for Foundational rows.

**Checkpoint**: The broker runs, events can be encoded, recorded in outboxes, published, consumed after commit, and parked; the new tables, baseline tooling, harness, and ledger exist; feature 005 behavior is unchanged.

---

## Phase 3: User Story 1 - Complete Checkout Through Broker-Carried Messages (Priority: P1) 🎯 MVP

**Goal**: Complete runs the choreographed Saga: T1 records the attempt and `CheckoutRequested`, Inventory applies or rejects and records its outcome event, and Transaction's outcome handler runs T3 or reopen and answers the waiting request. No checkout gRPC call remains.

**Independent Test**: With all services and the broker running, seed controlled stock, complete known baskets (including concurrently and with insufficient stock), and compare receipts, status, and stock with expected values; confirm from `outbox_event` rows and broker records that every Transaction–Inventory interaction was an event (US1 Independent Test).

### Tests for User Story 1 ⚠️ write first; they must fail before implementation

- [X] T046 [P] [US1] Create `server/tests/unit/transaction/SettlementRegistryResponderTest.java` (D7): a registered responder starts WAITING; exactly one of `claimForHandler` (WAITING → ANSWERING) and `claimTimeout` (WAITING → TIMED_OUT) succeeds under 10,000 concurrent races; an ANSWERING responder receives the settlement even if its budget expires during the handler's commit; an entry without a responder can still be completed.
- [X] T047 [P] [US1] Create `server/tests/unit/transaction/CheckoutOutcomeHandlerTest.java` for the healthy rules of [checkout-saga.md](contracts/checkout-saga.md#outcome-handler-rules-transaction-checkoutoutcomehandler): `StockDeducted` for the PENDING active attempt runs T3 and completes the future; `StockRejected` (`INSUFFICIENT_STOCK` with SKU, `INVALID_REQUEST`) and `StockDeductionRefused` reopen with reason and SKU; an outcome for an already settled attempt with the same result is a no-op; `RestorationNotNeeded` changes nothing; `client_outcome` is `SUCCESS` or `REJECTED` only when the handler claimed the responder, inside the same UPDATE; every action runs under the transaction's stripe lock and leaves the state cache equal to the database.
- [X] T048 [P] [US1] Create `server/tests/unit/inventory/CheckoutRequestHandlerTest.java`: a new `CheckoutRequested` applies all lines atomically and records `StockDeducted` in the same transaction; a basket with a short SKU records `StockRejected(INSUFFICIENT_STOCK, first short SKU in SKU order)` and changes no stock; an unknown SKU records `StockRejected(INVALID_REQUEST, sku)`; a redelivered request for a recorded attempt rolls back, writes nothing, and re-sends the recorded outcome by `outcome_event_id`; a reused attempt key with different lines throws `PermanentEventFailure(PAYLOAD_MISMATCH)`; a deadlock after internal retries is a `TransientEventFailure`, never `INSUFFICIENT_STOCK` (FR-033).
- [X] T049 [P] [US1] Adapt `server/tests/unit/transaction/CheckoutOrchestratorTest.java`: replace `FakeInventoryGateway` with `FakeInventoryReactor` (T058) on `InMemoryEventBus`; pre-check order 404 → 409 `TRANSACTION_NOT_OPEN` → 409 `EMPTY_BASKET` with no outbox row (FR-032); T1 writes the attempt and one `CheckoutRequested` outbox row with lines sorted by SKU, one per SKU; the request thread never calls the sender and waits at most `CHECKOUT_RESPONSE_BUDGET_MS` (8,000). Superseded assertions about the in-request retry loop and the 2 s RPC deadline are recorded `SUPERSEDED` with FR-031/FR-037.
- [X] T050 [P] [US1] Adapt `server/tests/database/JdbcCheckoutAttemptStoreTest.java`: T1 is one guarded UPDATE, the attempt INSERT with `request_event_id`, and the outbox INSERT, committed together or not at all; `next_retry_at = created_at + CHECKOUT_REPUBLISH_AFTER_MS` (2,000 ms); T3 and reopen are guarded UPDATEs that write `client_outcome` only when passed; `request_event_id` is not null.
- [X] T051 [P] [US1] Adapt `server/tests/database/JdbcCheckoutOperationStoreTest.java`: the apply transaction contains the locking SELECT in SKU order, the `checkout_operation` INSERT with a not-null `outcome_event_id`, the CASE UPDATE only when applied, and the outcome outbox INSERT, then one commit; a duplicate primary key `(transaction_id, attempt_number)` rolls back and returns the recorded operation with its `outcome_event_id`.
- [X] T052 [P] [US1] Adapt `server/tests/integration/CheckoutSagaTest.java` to run over `InMemoryEventBus` with real Transaction and Inventory business classes (US1 scenarios 1, 2, 5, 6): the two-SKU success with exact stock reduction and captured prices; insufficient stock giving 409 naming a SKU, no stock change, basket OPEN, then further scans and a successful later attempt (the end-to-end compensation path); an unknown-SKU checkout (the SKU is in Transaction's replica but not in Inventory's catalog) giving 500 `INTERNAL_ERROR`, the attempt REJECTED with `INVALID_REQUEST` and the SKU, the transaction OPEN, and no stock change (spec edge case, the `INVALID_REQUEST` compensation path); unknown transaction and empty basket publishing no event; a catalog price change after scan not affecting the receipt.
- [X] T053 [P] [US1] Adapt `server/tests/integration/ConcurrentCompletionTest.java`, `server/tests/integration/StockConservationTest.java`, and `server/tests/integration/DuplicateCompletionTest.java` to the event deployment (US1 scenarios 3, 4): two transactions racing for the last unit give one 200 and one 409 `INSUFFICIENT_STOCK`; stock never negative; initial minus final stock equals units in COMPLETED transactions; repeated Complete gives 409 `TRANSACTION_NOT_OPEN` with no further Inventory effect, including after restarting each service and the broker (live part gated by `KAFKA_TEST_BROKER_CONTROL`).
- [X] T054 [P] [US1] Create `server/tests/integration/CheckoutMessagesOnlyTest.java` (US1 scenario 7, FR-010): for successful and rejected checkouts, every record sent on the checkout and outcome topics has a committed `outbox_event` row in its producer's schema created before the send, linked from `checkout_attempt.request_event_id` or `checkout_operation.outcome_event_id`; Transaction's classpath contains no class that calls Inventory's `ApplyCheckout`.
- [X] T055 [P] [US1] Create `server/tests/messaging/CheckoutEventContractTest.java` against live Kafka (FR-014, replaces `rpc/InventoryCheckoutContractTest`): a real Transaction producer and a real Inventory consumer built separately from the same schema exchange `CheckoutRequested` → `StockDeducted` and → `StockRejected` with every field asserted; keys equal the transaction ID; envelopes carry the owner as `producer`. Record each replaced method in the ledger.
- [X] T056 [P] [US1] Adapt `server/tests/performance/CompleteStatementBudgetTest.java` and `server/tests/performance/InventoryApplyStatementBudgetTest.java`, create `server/tests/performance/OutcomeHandlerStatementBudgetTest.java`, and add keys to `server/tests/performance/statement-budgets.properties` (plan *Key flows*): `complete.t1.statements=3`, `complete.t1.commits=1`; `inventory.apply.statements=4`, `inventory.apply.commits=1`; `transaction.outcome.statements=2`, `transaction.outcome.commits=1`; each with a matching `.design` value.
- [X] T057 [P] [US1] Create `server/tests/performance/HeldResourcesDuringEventWaitTest.java` (FR-040, replaces `performance/HeldConnectionDuringRpcTest`) and adapt `server/tests/performance/HeldConnectionDuringWaitTest.java`: while a Complete waits for its outcome event (Inventory reactor delayed 3 s), the request thread holds no pool connection, no stripe lock, and no open transaction; Inventory's consumer holds a connection only inside its apply transaction.

### Implementation for User Story 1

- [X] T058 [P] [US1] Create `server/tests/support/FakeInventoryReactor.java`, which consumes `CheckoutRequested` from `InMemoryEventBus` and publishes scripted outcomes (deduct, reject with reason and SKU, refuse, delay, drop, duplicate), replacing `FakeInventoryGateway` for checkout.
- [X] T059 [US1] Adapt `server/tests/support/CheckoutDeployment.java`, `server/tests/support/InProcessCheckout.java`, `server/tests/support/InProcessTransaction.java`, and `server/tests/support/InProcessInventory.java` to wire `InMemoryEventBus` (default) or live Kafka (`KAFKA_TEST_BOOTSTRAP` with a per-run prefix) between Transaction and Inventory, with outbox dispatchers and consumer loops started and stopped with the deployment.
- [X] T060 [US1] Add `request_event_id` CHAR(36) NOT NULL to `checkout_attempt` in `db/transaction/schema.sql`, and adapt `server/transaction/src/database/CheckoutAttemptStore.java` and `server/transaction/src/database/JdbcCheckoutAttemptStore.java` (makes T050 pass): `begin` takes the `OutboxRow` and inserts it in the T1 transaction; `next_retry_at = created_at + CHECKOUT_REPUBLISH_AFTER_MS`; `recordCompletion` and `reopen` accept an optional `client_outcome`; `last_error` accepts `REPUBLISHED`, `PUBLISH_FAILED`, `STUCK`, and `INTEGRITY:<reason>`; `rejection_reason` keeps `INSUFFICIENT_STOCK`, `INVALID_REQUEST`, `STOCK_RESTORED`.
- [X] T061 [P] [US1] Add `outcome_event_id` CHAR(36) NOT NULL and `restoration_event_id` CHAR(36) NULL to `checkout_operation` in `db/inventory/schema.sql`, and adapt `server/inventory/src/database/CheckoutOperationStore.java` and `server/inventory/src/database/JdbcCheckoutOperationStore.java` (makes T051 pass): the apply transaction inserts the outcome `OutboxRow` and stores its `event_id`; a duplicate returns the recorded operation including `outcome_event_id`; a BLOCKED tombstone also gets an `outcome_event_id`.
- [X] T062 [P] [US1] Create `server/transaction/src/transaction/CheckoutEvents.java`: builds the `CheckoutRequested` envelope and `OutboxRow` for an attempt (UUID `event_id`, key and correlation ID = transaction ID, `aggregate_key = <transaction_id>:<attempt_number>`, lines sorted by SKU with one per SKU, producer `transaction`), and decodes outcome payloads into business records.
- [X] T063 [P] [US1] Create `server/inventory/src/inventory/StockOutcomeEvents.java`: builds `StockDeducted`, `StockRejected` (reason, first short or unknown SKU in SKU order), and `StockDeductionRefused` envelopes and `OutboxRow`s (producer `inventory`, same key and `aggregate_key` as the request), and decodes `CheckoutRequested`.
- [X] T064 [US1] Adapt `server/transaction/src/transaction/SettlementRegistry.java` (D7, makes T046 pass): each entry gains a responder state WAITING, ANSWERING, or TIMED_OUT changed only by compare-and-set; `register(attemptKey)`, `claimForHandler`, `claimTimeout`, and `complete(settlement)`; the registry stays process-local and never authoritative.
- [X] T065 [US1] Create `server/transaction/src/transaction/CheckoutOutcomeHandler.java` implementing `EventHandler` for the healthy rules (makes T047 pass): under the stripe lock, read the attempt; for the active PENDING attempt, claim the responder, run T3 (`StockDeducted`) or reopen (`StockRejected`, `StockDeductionRefused`) through `CheckoutAttemptStore` with `client_outcome` when claimed, update `TransactionStateCache`, and complete the registry future; settled attempts are no-ops; transient store failures throw `TransientEventFailure`. Fault hooks `checkout.outcome.beforeCommit` and `checkout.afterRecordCompletion`.
- [X] T066 [US1] Adapt `server/transaction/src/transaction/CheckoutOrchestrator.java` (makes T049 pass): keep the 005 pre-checks; T1 also inserts the `CheckoutRequested` outbox row from `CheckoutEvents`; after the commit hit `checkout.afterBegin`, signal the dispatcher, register as responder, and wait on the future for the rest of the 8 s budget holding nothing; on budget end claim TIMED_OUT, commit `client_outcome = PENDING_ERROR` and `next_retry_at = now`, hand the attempt to the recovery schedule, and throw `CheckoutPendingFailure` (500); if the handler claimed first, answer from its settlement (200 receipt from the cached snapshot and supplied `completed_at`, 409 `INSUFFICIENT_STOCK` naming the SKU, or 500 for `INVALID_REQUEST`/`STOCK_RESTORED`). Remove the in-request `InventoryGateway.applyCheckout` call, its retry loop, and the RPC deadline.
- [X] T067 [US1] Adapt `server/transaction/src/transaction/CheckoutRecoveryWorker.java` and `server/transaction/src/transaction/RecoverySchedule.java`: a due attempt is read by key, dropped if settled, and otherwise its recorded `CheckoutRequested` is re-sent through `OutboxSignal.resend(request_event_id)` (implemented by `OutboxDispatcher`); then `next_retry_at` backs off from 2 s to `CHECKOUT_REPUBLISH_MAX_MS` (5 s) with no limit, `retry_count` increments, and `last_error = REPUBLISHED` (or `PUBLISH_FAILED` on send failure). The worker never settles an attempt. Remove its `applyCheckout` call.
- [X] T068 [US1] Adapt `server/inventory/src/inventory/CheckoutStockService.java` (makes T048's service-level assertions pass): the apply transaction writes the outcome `OutboxRow` from `StockOutcomeEvents`; after the commit hit `inventory.apply.afterCommit` and signal the dispatcher; on a duplicate, compare stored lines (mismatch → `PermanentEventFailure(PAYLOAD_MISMATCH)`) and otherwise `resend(outcome_event_id)`; RESTORED or BLOCKED records answer with their recorded `StockDeductionRefused`; keep the 005 deadlock retry and treat persistent faults as transient.
- [X] T069 [US1] Create `server/inventory/src/inventory/CheckoutRequestHandler.java` implementing `EventHandler`: decodes `CheckoutRequested`, validates lines (positive quantities, one per SKU, sorted), and calls `CheckoutStockService` (makes T048 pass).
- [X] T070 [P] [US1] Create `server/inventory/src/messaging/CheckoutEventConsumer.java`: a `ConsumerLoop` on `selfcheckout.checkout.v1`, group `inventory-checkout`, `CHECKOUT_CONSUMER_THREADS` (8) threads, dispatching by `event_type` (`CheckoutRequested` → `CheckoutRequestHandler`; `StockRestorationRequested` added in T094), parking into Inventory's `dead_letter_event`.
- [X] T071 [P] [US1] Create `server/transaction/src/messaging/CheckoutOutcomeConsumer.java`: a `ConsumerLoop` on `selfcheckout.checkout-outcome.v1`, group `transaction-checkout-outcome`, 8 threads, calling `CheckoutOutcomeHandler`, parking into Transaction's `dead_letter_event`.
- [X] T072 [US1] Wire `server/inventory/src/inventory/InventoryMain.java`: create the outcome producer (`linger.ms=0`), registered for the outcome topic in the dispatcher's topic-to-sender map (T028), `JdbcOutboxStore`, `JdbcDeadLetterStore`, `OutboxDispatcher`, and `CheckoutEventConsumer` from `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_PREFIX`, `CHECKOUT_CONSUMER_THREADS`, `OUTBOX_SWEEP_MS`, `OUTBOX_MARK_BATCH_MS`, and `CONSUMER_MAX_ATTEMPTS`; start them after the pool, never blocking startup on the broker; keep the gRPC server (catalog) until T111.
- [X] T073 [US1] Wire `server/transaction/src/transaction/TransactionMain.java`: create the checkout producer (`linger.ms=0`), `JdbcOutboxStore`, `JdbcDeadLetterStore`, `OutboxDispatcher`, `CheckoutOutcomeConsumer`, and the adapted orchestrator and recovery worker; read `CHECKOUT_REPUBLISH_AFTER_MS` (2000) and `CHECKOUT_REPUBLISH_MAX_MS` (5000); remove `applyCheckout` and `checkoutOutcome` from `server/transaction/src/transaction/InventoryGateway.java` and `server/transaction/src/rpc/GrpcInventoryGateway.java`, leaving only the catalog methods until T110.
- [X] T074 [US1] Update `server/tests/support/FakeInventoryGateway.java` to keep only the catalog methods, and move every checkout use in tests to `FakeInventoryReactor`. Land this in the same change as T073, so the test tree compiles and passes at the end of US1 (FR-065). It adapts these files, each recorded `ADAPTED` in the 006 ledger with FR-031:
  - `unit/transaction/CheckoutRecoveryWorkerTest`: republish through `FakeInventoryReactor`; T089 adds the US3 assertions later.
  - `integration/BoundedWaitTest` and `performance/MultiInstanceTransactionTest`: outcomes through `FakeInventoryReactor`; T084 and T088 add the US3 assertions later.
  - `performance/TransactionCacheCorrectnessTest` and `unit/transaction/TransactionServiceTest`: scripted deductions and rejections through `FakeInventoryReactor`.
  - `performance/AmbiguousCommitReconcileTest`: the ambiguous T1 and T3 commit cases stay; the case that reconciled through `applyCheckout` becomes a dropped outcome settled by republish (FR-037).
  - The inline gateway fakes in `unit/transaction/ProductCatalogReplicaTest` and `performance/ScanStatementBudgetTest`: drop their `applyCheckout` and `checkoutOutcome` overrides.
- [X] T075 [US1] Run T046–T057 until they pass; then run the full suite (`./build-tests.sh && ./run-tests.sh`) with the live broker, fixing regressions without weakening assertions.
- [X] T076 [US1] Update the US1 rows of the 006 ledger in `server/tests/COVERAGE-LEDGER.md`: `rpc/InventoryCheckoutContractTest` methods `MIGRATED` to `messaging/CheckoutEventContractTest` and `integration/CheckoutSagaTest`; `performance/HeldConnectionDuringRpcTest` `MIGRATED` to `performance/HeldResourcesDuringEventWaitTest`; adapted files `ADAPTED`; in-request retry assertions `SUPERSEDED` (FR-031, FR-037).
- [X] T077 [US1] US1 gate: `PARITY_GATE=006:US1 ./run-tests.sh` passes with the live broker; fill `Verified at`.
- [X] T078 [US1] Smoke the event-based checkout through the gateway: start the broker, `./run-inventory.sh`, `./run-transaction.sh`, `./run-analytics.sh`, and Nginx, and run quickstart scenario A ([quickstart.md](quickstart.md#5-scenarios)); confirm `outbox_event` rows in both schemas have `published_at` set.
- [X] T079 [US1] Run CP1 (Saga over events, gRPC catalog and scans still present) with `server/benchmark-event-based.ps1 -Workload default -OutputLabel cp1-default-1` and `-Workload stress -OutputLabel cp1-stress-1`, record both in `load-client/reports/event-based-architecture/checkpoints.md` with a note that CP1 is taken at the end of US1, before the US3 fault and wait suites (plan stage 3), because US3 adds no work on the request or consumer hot path; investigate any drop of more than 5% from CP0 before continuing (plan *Checkpoints*), using the `outbox.*`, `consumer.*`, and Complete wait timers.

**Checkpoint**: MVP. Checkout runs over broker-carried domain events with exact stock and receipts; CP1 is recorded.

---

## Phase 4: User Story 3 - Settle Every Checkout Exactly Once Despite Message and Broker Faults (Priority: P1)

**Goal**: Every checkout interrupted by a crash, lost, duplicated, delayed, or redelivered event, broker restart, or outage settles exactly once without manual repair; integrity faults stay pending with an alarm; the reserved restoration works and is verified directly.

**Independent Test**: Inject faults at each Saga step with the fault hooks and the in-memory bus, crash and restart each service and the broker, and verify settled state, client responses, and stock reconciliation; run the direct restoration suite and report it separately (US3 Independent Test).

### Tests for User Story 3 ⚠️ write first; they must fail before implementation

- [X] T080 [P] [US3] Create `server/tests/integration/CheckoutEventFaultTest.java` (replaces `integration/CheckoutRecoveryFaultTest`): for each row of the [fault matrix](contracts/checkout-saga.md#crash-message-and-broker-fault-matrix), run real service processes through `support/ServiceProcesses` with a `halt` hook (`checkout.afterBegin`, `outbox.beforeSend` and `outbox.afterConfirm` in each producer, `inventory.apply.afterCommit`, `consumer.afterHandlerCommit` in Inventory and Transaction, `checkout.outcome.beforeCommit`, `checkout.afterRecordCompletion`), restart the halted service, and assert that the attempt settles COMPLETED exactly when Inventory applied, stock changes once, no restoration occurs, the client never saw success for an unsettled attempt, and lookup shows the settled state (US3 scenarios 1, 2, 4).
- [X] T081 [P] [US3] Create `server/tests/integration/CheckoutMessageFaultTest.java` on `InMemoryEventBus`: duplicate every checkout and outcome event; drop `CheckoutRequested` before consumption; drop the outcome; delay each by more than the budget; reorder across partitions; redeliver after a lost acknowledgement; pause the bus past the deadline. Every attempt settles once through republish, a stale outcome for an earlier attempt never settles a newer one, and after a randomized combination of these faults over 200 transactions, initial minus final stock equals units in COMPLETED transactions and no stock is negative (US3 scenarios 2, 5, 10; SC-003).
- [X] T082 [P] [US3] Create `server/tests/messaging/ConsumerCrashAfterCommitTest.java` against live Kafka: halt Inventory's and then Transaction's consumer at `consumer.afterHandlerCommit`; after restart the redelivered event has no second effect, Inventory re-sends the recorded outcome, and the attempt settles once (quickstart scenario F).
- [X] T083 [P] [US3] Create `server/tests/messaging/BrokerRestartCheckoutTest.java` against live Kafka with `KAFKA_TEST_BROKER_CONTROL`: with 50 checkouts in flight, restart the broker immediately and, separately, stop it for 10 s before starting it (on Windows every stop is a forced kill, D1); no confirmed event is lost, unconfirmed rows are re-sent from outboxes, every pending attempt settles automatically, and no stock effect applies twice (US3 scenario 3, SC-005).
- [X] T084 [P] [US3] Adapt `server/tests/integration/BoundedWaitTest.java` and create `server/tests/integration/CheckoutBrokerOutageTest.java` (FR-042, FR-044): with the bus paused or Inventory stopped, Complete returns 500 within 10 s and leaves the attempt PENDING with `client_outcome = PENDING_ERROR`; after recovery it settles COMPLETED (or OPEN when the stock was taken meanwhile), lookup shows it, and a repeated Complete gives 409 `TRANSACTION_NOT_OPEN` with no stock change; with the bus refusing sends (the broker's storage full or publishes refused, spec edge case), outbox rows and scans stay unpublished, Scan keeps succeeding, Complete behaves as with the broker down, and every row is sent exactly once after sends are accepted again (FR-015, FR-037); a Scan or Complete for a pending transaction waits up to the bound, then gives 409, normal processing, or 500 with no change, holding no connection (US3 scenarios 5–7).
- [X] T085 [P] [US3] Create `server/tests/integration/CheckoutIntegrityAlarmTest.java` (FR-038, US3 scenario 8): a `CheckoutRequested` with reused attempt key and different lines is parked in Inventory as `PAYLOAD_MISMATCH`; a `StockDeducted` for a REJECTED attempt, a `StockRestored` for a COMPLETED transaction, and an outcome for an unknown attempt are parked in Transaction (`INTEGRITY`, `UNKNOWN_ATTEMPT`) with `last_error = INTEGRITY:<reason>`; the affected attempt stays PENDING and is reported as stuck after `CHECKOUT_STUCK_ALARM_SECONDS` (set to 2 s in the test); other transactions' events keep flowing; parked rows appear in the status queries.
- [X] T086 [P] [US3] Create `server/tests/messaging/RestorationEventDirectTest.java` against Inventory Service only (replaces `rpc/RestoreCheckoutDirectTest`, FR-039, SC-016): write each `StockRestorationRequested` into Transaction's `outbox_event` (test schema) with `JdbcOutboxStore` and send it with a Transaction-side `OutboxDispatcher`, as the owning producer would (FR-010; still no production code path produces it), and assert that units are restored exactly once across duplicates, repeats, an Inventory halt at `inventory.restore.afterCommit` with redelivery after restart, and a restoration delivered before a delayed `CheckoutRequested` (which then gets `StockDeductionRefused` and has no effect); `NOTHING_TO_RESTORE` blocks later deductions; `ALREADY_RESTORED` changes nothing. `run-tests.sh` reports it in its separate direct-restoration section (T008).
- [X] T087 [P] [US3] Adapt `server/tests/integration/BackgroundCompletionAccountingTest.java`: under budget races between the handler claim and the timeout claim, exactly one of them writes `client_outcome`; background completions (`state = COMPLETED AND client_outcome = PENDING_ERROR`) and completions without a recorded response are counted exactly.
- [X] T088 [P] [US3] Adapt `server/tests/performance/MultiInstanceTransactionTest.java` (D11, FR-044): two Transaction processes share the outcome group; outcomes consumed by the process not holding the request settle durably; the waiter answers from the database backstop (`CHECKOUT_WAIT_BACKSTOP_MS` = 1,000) or returns 500; a repeat gives 409 with no further deduction; every durable guarantee holds.
- [X] T089 [P] [US3] Adapt `server/tests/unit/transaction/CheckoutRecoveryWorkerTest.java`: republish schedule 2 s → 5 s cap indefinitely; startup scan of every PENDING attempt and the 30 s safety scan; budget-end handovers due at once; never settles; stuck alarm at 60 s sets `last_error = STUCK` without changing state.
- [X] T090 [P] [US3] Adapt `server/tests/database/ServiceStatusQueriesTest.java`: Transaction reports `unpublished_outbox`, `parked_events`, `stuck_attempts`; Inventory reports `unpublished_outbox` and `parked_events`.

### Implementation for User Story 3

- [X] T091 [US3] Extend `server/transaction/src/transaction/CheckoutOutcomeHandler.java` with the integrity rules (makes T085 pass): `StockDeducted` for a REJECTED attempt and `StockRestored` for a COMPLETED transaction throw `PermanentEventFailure(INTEGRITY)` after recording `last_error = INTEGRITY:<reason>` in a separate local transaction; an unknown attempt throws `PermanentEventFailure(UNKNOWN_ATTEMPT)`; `StockRestored` for the PENDING active attempt reopens with reason `STOCK_RESTORED`; a transient T3 commit failure is retried under the lock (005 rule), and when retries are exhausted the claimed request answers 500 from a failed settlement and the handler throws `TransientEventFailure` so the offset is not committed.
- [X] T092 [US3] Extend `server/transaction/src/transaction/CheckoutRecoveryWorker.java` (makes T089 pass): count attempts PENDING longer than `CHECKOUT_STUCK_ALARM_SECONDS` (60) as stuck, set `last_error = STUCK`, and log each with its age once per status interval; keep the startup scan and 30 s safety scan (`CHECKOUT_RECOVERY_SCAN_SECONDS`).
- [X] T093 [P] [US3] Create `server/inventory/src/inventory/StockRestorationHandler.java` and extend `server/inventory/src/inventory/CheckoutStockService.java` and `server/inventory/src/inventory/StockOutcomeEvents.java` (D8): the 005 idempotent restore writes one `StockRestored` or `RestorationNotNeeded(NOTHING_TO_RESTORE | ALREADY_RESTORED)` outbox row in the same transaction and stores `restoration_event_id`; a repeated restoration re-sends the recorded event; fault hook `inventory.restore.afterCommit`. No production code produces `StockRestorationRequested`.
- [X] T094 [US3] Register `StockRestorationRequested` → `StockRestorationHandler` in `server/inventory/src/messaging/CheckoutEventConsumer.java`.
- [X] T095 [P] [US3] Update `server/tests/support/FaultInjection.java` and `server/tests/support/ServiceProcesses.java` with the hook names of [checkout-saga.md](contracts/checkout-saga.md#test-only-fault-hooks) and helpers to start a service with `-Dselfcheckout.faults=<hook>:halt` and a live broker configuration.
- [X] T096 [US3] Extend `server/transaction/src/database/TransactionStatusQueries.java` and `server/inventory/src/database/InventoryStatusQueries.java` and their status lines in `TransactionMain` and `InventoryMain` (FR-046, makes T090 pass): Transaction `unpublished_outbox`, `parked_events`, `stuck_attempts`, `outcome_consumer_lag`; Inventory `unpublished_outbox`, `parked_events`, `checkout_consumer_lag` (lags from `ConsumerLoop.lag()`).
- [X] T097 [US3] Run T080–T090 until they pass; then run the full suite with the live broker. Run `messaging.RestorationEventDirectTest` alone and record its result separately in `load-client/reports/event-based-architecture/quickstart-validation.md` under "Direct restoration (SC-016)".
- [X] T098 [US3] Update the US3 ledger rows: `rpc/RestoreCheckoutDirectTest` `MIGRATED` to `messaging/RestorationEventDirectTest`; `integration/CheckoutRecoveryFaultTest` `MIGRATED` to `integration/CheckoutEventFaultTest`, then delete `server/tests/integration/CheckoutRecoveryFaultTest.java`; adapted files `ADAPTED`.
- [X] T099 [US3] US3 gate: `PARITY_GATE=006:US3 ./run-tests.sh` passes with the live broker and `KAFKA_TEST_BROKER_CONTROL=1`; run quickstart scenarios F and G and record them; fill `Verified at`.

**Checkpoint**: The Saga is fault tolerant over events: every interrupted checkout settles exactly once, integrity faults are parked and alarmed, and restoration is verified directly.

---

## Phase 5: User Story 4 - Keep Catalog Replicas Current Through Inventory's Events (Priority: P2)

**Goal**: Inventory publishes its full catalog through the outbox at every startup onto the compacted topic, and Transaction replays it from offset 0 and swaps its durable replica on a complete versioned snapshot. No catalog RPC remains, and Inventory's gRPC server is removed.

**Independent Test**: Reset and seed, start Transaction before and after Inventory, reseed with a different catalog, and restart Transaction while the broker is down; the replica converges through events only and keeps serving (US4 Independent Test).

### Tests for User Story 4 ⚠️ write first; they must fail before implementation

- [X] T100 [P] [US4] Create `server/tests/unit/transaction/CatalogEventHandlerTest.java` (D9): items are buffered per `catalog_version`; a `CatalogSnapshotPublished` for a new version replaces the replica in one local transaction only when the number of distinct SKUs buffered for that version equals `product_count`; a repeated record of the same SKU and version (left by an earlier Inventory startup that compaction has not yet removed) replaces the buffered item and is not counted twice; a marker with fewer distinct SKUs does nothing; a marker for the current version is a no-op; records of other versions, including leftovers compaction keeps under removed SKUs, are ignored; prices parse as two-fraction-digit decimals.
- [X] T101 [P] [US4] Adapt `server/tests/unit/transaction/ProductCatalogReplicaTest.java` and `server/tests/database/JdbcProductReplicaStoreTest.java`: the replica and `product_replica_meta` are replaced atomically from a snapshot, and the in-memory snapshot swaps only after the commit; no gRPC refresh path remains.
- [X] T102 [P] [US4] Create `server/tests/integration/CatalogReplicationTest.java` on `InMemoryEventBus` (US4 scenarios 1–4): a fresh seed reaches Transaction's replica with no synchronous call; Transaction started after the events were published still converges; a reseed with a different catalog converges and a late record of the older version never overwrites the newer replica; a reseed with the same catalog leaves the replica content unchanged; with the bus paused and Inventory stopped, a restarted Transaction serves scans from the durable replica; with no replica, Scan returns 500 `INTERNAL_ERROR` (FR-030).
- [X] T103 [P] [US4] Create `server/tests/messaging/CatalogEventContractTest.java` against live Kafka (replaces `rpc/InventoryCatalogContractTest`): a real Inventory startup writes 2,000 `CatalogItemPublished` rows and one `CatalogSnapshotPublished` in one outbox transaction and sets `catalog_meta.published_at`; the records land on the compacted single-partition topic with the marker after its items; a real Transaction replays from offset 0 and reports `replica_version` equal to Inventory's `catalog_version` within 5 s; with `KAFKA_TEST_BROKER_CONTROL`, a broker restart while Transaction is replaying still ends with `replica_version` equal to `catalog_version` and no partial replica swap (SC-005).
- [X] T104 [P] [US4] Adapt `server/tests/database/InventoryReadAdaptersTest.java` for `catalog_meta.published_at`, and add a catalog-publication budget (`inventory.catalog.publish.commits=1`, with `.design`) to `server/tests/performance/statement-budgets.properties` enforced in `server/tests/performance/InventoryApplyStatementBudgetTest.java`.

### Implementation for User Story 4

- [X] T105 [P] [US4] Add `published_at` TIMESTAMP(3) NULL to `catalog_meta` in `db/inventory/schema.sql`, and extend `server/inventory/src/database/CatalogMetaStore.java` and `server/inventory/src/database/JdbcCatalogMetaStore.java` with `markPublished(uow, at)`.
- [X] T106 [US4] Create `server/inventory/src/inventory/CatalogPublisher.java`: at startup, one local transaction inserts 2,000 `CatalogItemPublished` outbox rows (key SKU; SKU, name, price as a two-fraction-digit string, `catalog_version`; correlation ID = version; null `aggregate_key`) followed by one `CatalogSnapshotPublished` (key `__snapshot__`; version and `product_count`) using batched inserts, sets `catalog_meta.published_at`, commits, and signals the dispatcher with the IDs in `outbox_seq` order. The catalog topic gets its own producer with `linger.ms=5`, created in `InventoryMain` (T109) and registered for the catalog topic in Inventory's dispatcher map (T028).
- [X] T107 [US4] Create `server/transaction/src/transaction/CatalogEventHandler.java` implementing `EventHandler` (makes T100 pass) with the version buffer (data-model *Catalog version buffer*), replacing the replica through `JdbcProductReplicaStore` and swapping `ProductCatalogReplica` after the commit.
- [X] T108 [P] [US4] Create `server/transaction/src/messaging/CatalogConsumer.java`: a manual-assignment `ConsumerLoop` on partition 0 of `selfcheckout.catalog.v1`, seeking to offset 0 at every start and tailing, parking into Transaction's `dead_letter_event`.
- [X] T109 [US4] Wire `CatalogPublisher` into `server/inventory/src/inventory/InventoryMain.java` (after the pool and dispatcher start, before consumers) and `CatalogConsumer` into `server/transaction/src/transaction/TransactionMain.java`; Transaction loads the durable replica first so Scan works before the topic is read.
- [X] T110 [US4] Remove the catalog RPC path: delete `server/transaction/src/transaction/CatalogReplicaRefresher.java`, `server/transaction/src/transaction/InventoryGateway.java`, `server/transaction/src/rpc/GrpcInventoryGateway.java`, and `server/transaction/src/transaction/DependencyUnavailableFailure.java` (and its mapping in `server/transaction/src/api/ApiErrorMapper.java`); remove `CATALOG_REFRESH_SECONDS`, `INVENTORY_GRPC_TARGET`, and `INVENTORY_RPC_DEADLINE_MS` from `TransactionMain`; keep `ProductCatalogUnavailableFailure` mapped to 500.
- [X] T111 [US4] Remove Inventory's gRPC surface: delete `server/inventory/src/rpc/InventoryGrpcService.java`, the gRPC server start and stop and `INVENTORY_GRPC_PORT` (9092) in `InventoryMain`, and the catalog-version check in `server/inventory/src/inventory/CatalogCache.java`. T110, T111, and T113 land as one change so the test tree keeps compiling.
- [X] T112 [US4] Extend status (FR-046): Transaction `replica_version` in `TransactionStatusQueries`; Inventory `catalog_version` and `catalog_published_at` in `InventoryStatusQueries`; and in `server/benchmark-event-based.ps1`'s readiness step, wait until Transaction's `replica_version` equals Inventory's `catalog_version` and every group in the harness's `$ConsumerGroups` list has its partitions assigned (SC-009). The list starts with the two checkout groups; T126 adds `analytics-scan-ingest`.
- [X] T113 [US4] Delete `server/tests/support/FakeInventoryGateway.java` and the three Inventory gRPC test files `server/tests/rpc/InventoryCheckoutContractTest.java`, `server/tests/rpc/RestoreCheckoutDirectTest.java`, and `server/tests/rpc/InventoryCatalogContractTest.java`, after confirming their ledger rows are `MIGRATED` with passing replacements; mark `rpc/InventoryCatalogContractTest` rows `MIGRATED` to `messaging/CatalogEventContractTest` and `integration/CatalogReplicationTest`. In the same change, adapt the remaining users of the types T110 deletes, and record them in the ledger:
  - `performance/ScanStatementBudgetTest`: seed the replica through `CatalogEventHandler` instead of `CatalogReplicaRefresher` and an `InventoryGateway` fake (`ADAPTED`, FR-017); T135 adds the US2 assertions later.
  - `unit/api/TransactionHttpHandlerTest`: the `DependencyUnavailableFailure` case becomes `SUPERSEDED` (FR-024), because that failure no longer exists; every other sanitized-500 case stays.
- [X] T114 [US4] Run T100–T104 until they pass, run the full suite with the live broker, run quickstart scenario H, and pass the US4 gate `PARITY_GATE=006:US4 ./run-tests.sh`; fill `Verified at`.

**Checkpoint**: The catalog replica is fed only by Inventory's events; Transaction no longer talks to Inventory at all.

---

## Phase 6: User Story 5 - Recover Accurate Popular Items From Broker-Delivered Scans (Priority: P2)

**Goal**: The scan relay publishes `ScanAccepted` to the single-partition topic; Analytics consumes it with its database as the authority for consumption progress, assigning positions and publishing windows exactly as before. Analytics' gRPC surface is removed.

**Independent Test**: Submit a controlled scan stream, inject duplicates, redelivery, and reordering, crash Analytics at each boundary, crash Transaction during publication, restart the broker mid-stream, and compare every window with an independent calculation (US5 Independent Test).

### Tests for User Story 5 ⚠️ write first; they must fail before implementation

- [X] T115 [P] [US5] Create `server/tests/messaging/ScanEventContractTest.java` against live Kafka (replaces `rpc/AnalyticsIngestContractTest`): a real `KafkaScanEventSink` and a real `ScanAcceptedConsumer` exchange `ScanAccepted` with every field asserted (`event_id = scan:<transaction_id>:<scan_sequence>`, captured name and price, acceptance time); `Delivered` is returned only after every confirmation.
- [X] T116 [P] [US5] Adapt `server/tests/database/JdbcScanIngestStoreTest.java`: the ingestion transaction also UPSERTs `consumer_offset(topic, partition, next_offset = last record offset + 1)` and commits it with the events and positions, or not at all; parked records' offsets are included in the next stored `next_offset`.
- [X] T117 [P] [US5] Adapt `server/tests/integration/ScanEventDeliveryTest.java`, `server/tests/integration/AnalyticsIngestCrashTest.java`, and `server/tests/integration/AnalyticsCrashRecoveryTest.java` to broker delivery: crashes at `relay.afterConfirm`, `analytics.ingest.received`, `analytics.ingest.beforeCommit`, and `analytics.ingest.afterCommit`, and Analytics halts at window computation and persistence, still give every accepted scan exactly one position, positions 1..N without gaps, and every eligible window once (US5 scenarios 3–5).
- [X] T118 [P] [US5] Create `server/tests/integration/AnalyticsOffsetAuthorityTest.java` (FR-049): with the broker group's committed offsets deleted or reset to 0 while Analytics keeps its database, Analytics resumes from `consumer_offset` with no loss or double count; with a stored offset beyond the log end (topic recreated), it logs an alarm, seeks to earliest, and deduplication absorbs the overlap.
- [X] T119 [P] [US5] Create `server/tests/messaging/BrokerRestartScanTest.java` with `KAFKA_TEST_BROKER_CONTROL`: an immediate restart and a 10 s stop before start, mid-stream of 20,000 scans; zero lost and zero double-counted scans; windows match an independent calculation (SC-005, SC-007).
- [X] T120 [P] [US5] Adapt `server/tests/integration/WindowBoundaryAccuracyTest.java` and `server/tests/integration/AnalyticsBacklogIsolationTest.java` to broker delivery (US5 scenarios 1, 2, 6–8): empty response below 1,000 scans; windows 1–1,000 then 501–1,500; backlog after an Analytics or broker outage published in order; more than ten tied SKUs give at most ten ranks by count then SKU; every ranked item carries the product name captured in its `ScanAccepted` (US4 scenario 5); scans of failed or never-completed transactions count.
- [X] T121 [P] [US5] Adapt `server/tests/performance/RelayStatementBudgetTest.java` to the final relay shape per batch of ≤ 500 (a SELECT on a connection released before sending; the batch send holding no connection; one UPDATE of `published_at` and a commit in a new short transaction) and `server/tests/performance/AnalyticsIngestStatementBudgetTest.java` (feature 005's statements plus one `consumer_offset` UPSERT; raise only the `.design` and ceiling of `analytics.ingest.statements` by exactly 1 in `statement-budgets.properties`).
- [X] T122 [P] [US5] Create `server/tests/integration/AnalyticsVisibilityLagTest.java` (FR-055, SC-008): under a steady healthy stream at the default workload's scan rate, the time from a boundary scan's commit to its window's publication is under 5 s at p99; after the stream stops, the highest eligible boundary is published within 60 s.

### Implementation for User Story 5

- [X] T123 [P] [US5] Create `server/transaction/src/messaging/KafkaScanEventSink.java` implementing `ScanEventSink` (D10): sends each row as one `ScanAccepted` record (key transaction ID, producer `transaction`, `linger.ms = SCAN_PRODUCER_LINGER_MS` = 5) with the deterministic event ID, waits for every confirmation, and returns `Delivered` (confirmed by the broker) or `Uncertain` on any failure. It is the only caller of `RecordSender` besides `OutboxDispatcher`.
- [X] T124 [US5] Adapt `server/analytics/src/analytics/ScanIngestionService.java`, `server/analytics/src/database/ScanIngestStore.java`, and `server/analytics/src/database/JdbcScanIngestStore.java` (makes T116 pass): ingestion takes decoded events with their topic, partition, and offsets, and UPSERTs `consumer_offset` in the same transaction; fault hooks `analytics.ingest.received`, `analytics.ingest.beforeCommit`, and `analytics.ingest.afterCommit` (before `commitAsync`).
- [X] T125 [US5] Create `server/analytics/src/messaging/ScanAcceptedConsumer.java`: a one-thread `ConsumerLoop` on `selfcheckout.scan-accepted.v1`, group `analytics-scan-ingest`, polling up to 500 records per batch and calling `ScanIngestionService` once per batch; on assignment, seek to `consumer_offset.next_offset` or 0; on out-of-range, alarm and seek earliest; park into Analytics' `dead_letter_event`.
- [X] T126 [US5] Wire `server/analytics/src/analytics/AnalyticsMain.java` and `server/transaction/src/transaction/TransactionMain.java`: Analytics starts `ScanAcceptedConsumer` and removes `AnalyticsIngestGrpcService`, its gRPC server, and `ANALYTICS_GRPC_PORT` (9093); Transaction's `ScanEventRelay` uses `KafkaScanEventSink` and removes `GrpcAnalyticsIngestClient`, `ANALYTICS_GRPC_TARGET`, and `ANALYTICS_RPC_DEADLINE_MS`. In the same change, restructure `server/transaction/src/transaction/ScanEventRelay.java` and `server/transaction/src/database/JdbcRelayStore.java`: the relay selects a batch, releases its pool connection before sending, waits for confirmations without holding a connection, then marks `published_at` in a new short transaction, with the new fault hook `relay.afterConfirm` (makes T117 and T121 pass). Add `analytics-scan-ingest` to `$ConsumerGroups` in `server/benchmark-event-based.ps1` (T112).
- [X] T127 [US5] Delete `server/analytics/src/rpc/AnalyticsIngestGrpcService.java` and `server/transaction/src/rpc/GrpcAnalyticsIngestClient.java`, and adapt `server/tests/support/SwitchableScanEventSink.java` and `server/tests/support/InProcessAnalytics.java` to the bus or live Kafka. In the same change, mark the `rpc/AnalyticsIngestContractTest` ledger rows `MIGRATED` to `messaging/ScanEventContractTest` and `integration/ScanEventDeliveryTest` and delete `server/tests/rpc/AnalyticsIngestContractTest.java`, and adapt `server/tests/integration/AnalyticsCheckoutIsolationTest.java` to feed Analytics through `KafkaScanEventSink` on the bus or live Kafka with its assertions unchanged (`ADAPTED`, FR-048), so the test tree keeps compiling.
- [X] T128 [US5] Extend `server/analytics/src/database/AnalyticsStatusQueries.java` and the Analytics status line with `parked_events`, `consumer_next_offset`, and `scan_consumer_lag` (FR-046).
- [X] T129 [US5] Run T115–T122 until they pass and run the full suite with the live broker; run quickstart scenario E and record it in `load-client/reports/event-based-architecture/quickstart-validation.md`.
- [X] T130 [US5] Confirm the US5 ledger rows in `server/tests/COVERAGE-LEDGER.md` and pass the US5 gate `PARITY_GATE=006:US5 ./run-tests.sh`; fill `Verified at`.
- [X] T131 [US5] Run CP2 (full event-based system) with `server/benchmark-event-based.ps1 -Workload default -OutputLabel cp2-default-1` and `-Workload stress -OutputLabel cp2-stress-1`; record in `checkpoints.md` with parked events and final lag; investigate a drop of more than 5% from CP1; evaluate D17's entry criteria (Inventory consumer lag p95 > 50 ms under stress; visibility p99 > 5 s) and record the result.

**Checkpoint**: Every inter-service interaction is an event; Analytics counts every accepted scan exactly once with its own durable progress; CP2 is recorded.

---

## Phase 7: User Story 2 - Scan Durably Without Waiting on the Broker or Any Peer (Priority: P1)

**Goal**: Start, Scan, and Lookup never touch the producer, a consumer, or a peer; with the broker, Inventory, or Analytics down, scanning continues, and every accepted scan reaches Analytics exactly once after they return.

**Independent Test**: Stop the broker, then start transactions, scan, look up, restart Transaction, and continue; start the broker and verify stock unchanged and every scan counted once; repeat with Inventory and then Analytics stopped (US2 Independent Test).

### Tests for User Story 2 ⚠️ write first; they must fail before implementation

- [X] T132 [P] [US2] Create `server/tests/integration/ScanWithoutBrokerTest.java` (US2 scenarios 1–3, SC-004): with the broker stopped (live, `deploy/kafka/stop-broker.ps1`) or the bus paused, Start, Scan, and Lookup succeed with correct names and prices, including after a Transaction restart; their latency does not include any `max.block.ms` wait; accepted scans stay unpublished in `accepted_scan`; after the broker returns, every scan is published and counted exactly once and stock is unchanged.
- [X] T133 [P] [US2] Adapt `server/tests/integration/ScanWithoutPeersTest.java`: with Inventory stopped, and then Analytics stopped, Start, Scan, and Lookup succeed; after each returns, scans are delivered once (SC-006, SC-007).
- [X] T134 [P] [US2] Adapt `server/tests/integration/TransactionRestartDurabilityTest.java` and `server/tests/integration/DurableBasketTest.java` (US2 scenario 4): after a Transaction halt and restart, lookup shows the same status, item count, total, and start time; completion uses every acknowledged unit; every acknowledged scan is still delivered to Analytics.
- [X] T135 [P] [US2] Adapt `server/tests/performance/ScanStatementBudgetTest.java`: the warm Scan stays a guarded UPDATE, an `accepted_scan` INSERT, and a commit; a counting `RecordSender` records zero sends on the request thread for Start, Scan, and Lookup (FR-015).
- [X] T136 [P] [US2] Extend `server/tests/unit/transaction/TransactionServiceTest.java` (US2 scenarios 5, 6): a rejected Scan for each error code commits neither a basket change nor an `accepted_scan` row; no Scan changes stock.

### Implementation for User Story 2

- [X] T137 [US2] Make startup and request paths broker-independent in `server/transaction/src/transaction/TransactionMain.java` and `server/common/src/messaging/KafkaClients.java`: the HTTP server starts whether or not the broker is reachable; producers and consumers connect in the background; nothing on Start, Scan, or Lookup references the dispatcher, the sink, or a consumer.
- [X] T138 [US2] Harden `server/transaction/src/transaction/ScanEventRelay.java` for broker outages, keeping the relay shape from T126: on `Uncertain`, back off from 100 ms to 5 s, and confirm that a broker outage never consumes pool connections needed by Scan (makes T132 pass).
- [X] T139 [US2] Run T132–T136 until they pass, run the full suite, run quickstart scenarios B and D and record them in `load-client/reports/event-based-architecture/quickstart-validation.md`, and pass the US2 gate `PARITY_GATE=006:US2 ./run-tests.sh`; fill `Verified at`.

**Checkpoint**: Scan is durable and independent of the broker and every peer.

---

## Phase 8: User Story 6 - Keep One Unchanged Client Interface (Priority: P2)

**Goal**: The seven routes, the gateway, the OpenAPI document, and the Load Client are byte-identical; broker conditions map only onto the established 500 and 502 responses and reveal nothing about messaging.

**Independent Test**: Run the contract tests and the unchanged Load Client through the base URL; stop each service and the broker in turn and exercise every route (US6 Independent Test).

### Tests for User Story 6 ⚠️ write first; they must fail before implementation

- [X] T140 [P] [US6] Create `server/tests/integration/BrokerConditionErrorMappingTest.java` ([public-http-and-routing.md](contracts/public-http-and-routing.md), FR-016, FR-024): for each internal condition in the mapping table, the response status and `{error, message}` body match the established contract, and no body contains `kafka`, `topic`, `offset`, `partition`, `broker`, or any event type name (case-insensitive).
- [X] T141 [P] [US6] Adapt `server/tests/integration/GatewayServiceOutageTest.java` (US6 scenario 2, *Availability and Failure Behavior*): with the broker stopped, then each service stopped, every route behaves as specified (gateway 502 for the stopped owner; Complete 500 within 10 s after pre-checks when the broker or Inventory is down; other routes unaffected).
- [X] T142 [P] [US6] Adapt `server/tests/integration/DatabaseOutageContractTest.java`: when a service's own database is unavailable, its database-dependent routes return 500 without acknowledging writes, and its consumers commit no offsets until the database returns; other services keep working.

### Implementation for User Story 6

- [X] T143 [US6] Review `server/transaction/src/api/ApiErrorMapper.java`, `server/transaction/src/transaction/CheckoutPendingFailure.java`, and `server/transaction/src/transaction/ProductCatalogUnavailableFailure.java` so every new failure path maps to the existing 500 `INTERNAL_ERROR` body with an existing message; fix any path that leaks a messaging detail (makes T140 pass).
- [X] T144 [US6] Verify the public contract: `architecture/PipelineArchitectureTest`'s hashes for `spec/self-checkout-openapi.yaml` and `load-client/src/**` pass unchanged; `git diff 8891c57 -- spec/self-checkout-openapi.yaml load-client/src deploy/nginx.conf` is empty; `SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh contract` passes; a 20 s unchanged Load Client run through the gateway completes with zero errors (SC-001).
- [X] T145 [US6] Run T140–T142 until they pass, then pass the US6 gate `PARITY_GATE=006:US6 ./run-tests.sh`; fill `Verified at`.

**Checkpoint**: Clients cannot tell that a broker exists.

---

## Phase 9: User Story 7 - Operate and Reset Each Service and the Broker Independently (Priority: P2)

**Goal**: gRPC is gone from code, build, configuration, and classpaths; architecture tests forbid synchronous service-to-service calls and publishing outside the outbox; each service and the broker build, start, stop, restart, and reset on their own; one procedure produces the baseline.

**Independent Test**: Start the broker from its README; build and start each service alone; attempt cross-service data access; reset and seed each service and the broker; restart each service and then the broker during a workload (US7 Independent Test).

### Tests for User Story 7 ⚠️ write first; they must fail before implementation

- [X] T146 [P] [US7] Finalize `server/tests/architecture/MicroserviceBoundaryTest.java` (D14, FR-006, SC-010): forbid `io.grpc`, `java.net.http.HttpClient`, `java.net.Socket`, `URL.openConnection`, and any `*_GRPC_*`, `*_TARGET`, or peer host or port configuration key in service source and run scripts; forbid any `grpc` jar under `server/lib/`; keep the Kafka-only-in-`messaging` rule and the Redis/RabbitMQ/ActiveMQ ban.
- [X] T147 [P] [US7] Create `server/tests/architecture/OutboxOnlyPublishTest.java` (FR-010, SC-010): no class outside `common/src/events/OutboxDispatcher.java` and `transaction/src/messaging/KafkaScanEventSink.java` calls `RecordSender.send` or `KafkaProducer.send`; every event type in `EventTypes` has exactly one producer and appears in [event-catalog.md](contracts/event-catalog.md); every production class that builds an envelope for a type does so in its owner service.
- [X] T148 [P] [US7] Finalize `server/tests/architecture/LayerDependencyTest.java` (remove the `rpc` layer; `messaging` is the only adapter layer, and no business class imports it) and `server/tests/architecture/CompositionRootTest.java` (each `*Main` wires its dispatcher, consumers, and stores; no `rpc` types).
- [X] T149 [P] [US7] Adapt `server/tests/integration/IndependentServiceDeploymentTest.java` (US7 scenario 1, SC-012): each service builds with `./build.sh <service>` and starts alone while the broker runs, and also starts while the broker is down.
- [X] T150 [P] [US7] Adapt `server/tests/integration/DatabaseIsolationTest.java` (US7 scenario 2, SC-011): each service's credentials are denied on the other schemas' `outbox_event`, `dead_letter_event`, `consumer_offset`, and every 005 table.
- [X] T151 [P] [US7] Adapt `server/tests/integration/ResetBaselineTest.java` (US7 scenario 3, FR-059): `db/reset-baseline.ps1` refuses while a service port answers; afterwards Inventory holds 2,000 products at 10,000 units with threshold 50, Transaction holds nothing but its replica after startup, Analytics holds nothing, topics are empty, and no outbox or parked row remains.
- [X] T152 [P] [US7] Adapt `server/tests/integration/RestartDuringWorkloadTest.java` (US7 scenario 4, SC-012): restart each service, then the broker, during a 30 s workload; the others keep serving as specified, and afterwards stock conservation, scan exactly-once, and window correctness hold.

### Implementation for User Story 7

- [X] T153 [US7] Delete the remaining gRPC runtime: `server/common/src/rpc/GrpcChannels.java`, `server/common/src/rpc/GrpcServers.java`, `server/common/src/rpc/RpcStatusClassifier.java`, the now-empty `server/common/src/rpc/`, `server/transaction/src/rpc/`, `server/inventory/src/rpc/`, and `server/analytics/src/rpc/`; and `server/tests/unit/common/RpcStatusClassifierTest.java` with its ledger rows `SUPERSEDED` (FR-006). T153–T156 land as one change so the build and test tree keep compiling.
- [X] T154 [US7] Delete `server/contracts/src/main/proto/selfcheckout/inventory/v1/inventory_service.proto` and `server/contracts/src/main/proto/selfcheckout/analytics/v1/analytics_ingest.proto`; remove the gRPC BOM, gRPC dependencies, `os-maven-plugin` use for gRPC, the `protoc-gen-grpc-java` `compile-custom` goal, and the `lib/grpc` copy execution from `server/contracts/pom.xml`.
- [X] T155 [US7] Remove `lib/grpc/*` from `server/build.sh` (which now deletes `lib/grpc/` and verifies only `lib/messaging/`), `server/build-tests.sh`, `server/run-tests.sh`, and `server/run-{transaction,inventory,analytics}.sh`; remove the gRPC lines from `.gitignore`; run `./build.sh` and confirm `lib/grpc/` no longer exists.
- [X] T156 [US7] Delete `server/tests/rpc/ProtoCompatibilityTest.java`, `server/tests/rpc/baseline/descriptor-summary.txt`, and `server/tests/support/GrpcTestSupport.java` after marking their ledger rows `MIGRATED` to `messaging/EventSchemaCompatibilityTest` or `SUPERSEDED` (FR-006); remove the `rpc.` package handling from `server/run-tests.sh`.
- [X] T157 [US7] Implement the shutdown order in `TransactionMain`, `InventoryMain`, and `AnalyticsMain` ([broker-and-deployment.md](contracts/broker-and-deployment.md#start-stop-restart)): HTTP, then consumers (final `commitSync`), then the dispatcher (final mark), then background workers, then producers, then the pool.
- [X] T158 [US7] Update `server/README.md` (FR-056): per service build, start, stop, restart, ports (8081–8083, no gRPC ports), configuration table of [broker-and-deployment.md](contracts/broker-and-deployment.md#configuration-changes) including removed variables, database connection and credential handling, topics produced and consumed, status fields, and the baseline procedure; link `deploy/kafka/README.md`.
- [X] T159 [US7] Run T146–T152 until they pass, run the full suite with `STRICT_LIVE_GATES=1`, and pass the US7 gate `PARITY_GATE=006:US7 ./run-tests.sh`; fill `Verified at`.

**Checkpoint**: No synchronous service-to-service path, gRPC artifact, or peer address remains; each component operates and resets independently.

---

## Phase 10: User Story 8 - Reproduce Benchmark Evidence and the Architecture Diagram (Priority: P3)

**Goal**: Two genuine Load Client reports from the event-based deployment, each after a fresh baseline, with FR-061 counts and a comparison with feature 005; an architecture diagram that GitHub renders and that matches the event catalog.

**Independent Test**: Reset, run the default command, verify invariants; repeat with a separate reset for the stress command; inspect both reports, counts, and the diagram against the event catalog (US8 Independent Test).

### Tests for User Story 8 ⚠️ write first; they must fail before implementation

- [X] T160 [P] [US8] Create `server/tests/architecture/ArchitectureDiagramConsistencyTest.java` (FR-063, D16, SC-015): parse the Mermaid source in `docs/architecture/event-based-architecture.md`; assert that it contains Nginx (8080), the three services with ports, the three schemas, the Kafka broker, all four topics, every producer edge and consumer-group edge from `TopicNames` and the event catalog, and each service's outbox and dead-letter tables; assert that no edge connects two services directly; assert that `docs/architecture/event-based-architecture.svg` exists and is newer than or equal to the Mermaid source in git history.

### Implementation for User Story 8

- [X] T161 [P] [US8] Create `docs/architecture/event-based-architecture.md` with a Mermaid `flowchart` showing the gateway, services, schemas with their outbox and `dead_letter_event` tables, the broker with the four topics, producer arrows, and consumer-group arrows labeled with event types, plus a short legend and a link to the event catalog (makes T160 pass).
- [X] T162 [US8] Export `docs/architecture/event-based-architecture.svg` with Mermaid CLI at a pinned version (`npx -y @mermaid-js/mermaid-cli@<pinned> -i … -o …`, version recorded in the `.md`), and link both files from the root `README.md`.
- [X] T163 [US8] Run CP3 with `server/benchmark-compare.ps1` and the shared-machine settings (D15 streamlined policy: one screening pair per relevant workload; three confirmation pairs only at ≥ +5%): A/B `KAFKA_FLUSH_EVERY_MESSAGE` true vs false, `CHECKOUT_CONSUMER_THREADS` 4 vs 8, `SCAN_PRODUCER_LINGER_MS` 5 vs 20, and `OUTBOX_COALESCE_MS` 0 vs 100; record each in `checkpoints.md`; adopt an option only on measured evidence (and adopt flush-per-message as default only within ±3%, D17); update defaults in code, `deploy/kafka/start-broker.ps1`, and `server/README.md` accordingly. Designate the CP4 labels in `checkpoints.md` before running them.
- [X] T164 [US8] Run the official default CP4 run on an idle machine: `powershell -File server/benchmark-event-based.ps1 -Workload default -OutputLabel cp4-default-1` (fresh baseline inside the harness); require `evidence.json` with `comparable=true`, a passing validator, zero errors for each operation, zero background completions, zero parked events, and zero final lag (SC-013); otherwise diagnose and rerun under a new label.
- [X] T165 [US8] Run the official stress CP4 run after a separate fresh baseline: `powershell -File server/benchmark-event-based.ps1 -Workload stress -OutputLabel cp4-stress-1`; require a passing validator, zero parked events, and zero final lag; record and explain the background-completion count (FR-066).
- [X] T166 [US8] Run the CP0-versus-CP4 interleaved comparison with `server/benchmark-compare.ps1 -BaselineCommit 8891c57` (one pair per workload) and record it in `checkpoints.md`.
- [X] T167 [US8] Copy the designated client reports byte-identically to `load-client/reports/event-based-architecture/default-report.json` and `load-client/reports/event-based-architecture/stress-report.json`, verifying SHA-256 equality with the run directories' copies.
- [X] T168 [US8] Write `load-client/reports/event-based-architecture/README.md` (FR-061, FR-062, SC-014). Because the Load Client's JSON is unchanged and cannot carry settings, the README identifies each committed report with a table row giving its file name, SHA-256, run label, git commit, settings, and broker image, linked to its run directory's `evidence.json`. It also gives the commands, baselines, broker image and flush setting, machine and durability settings; for each report `totalTransactions`, throughput, per-operation success and error rates, p95/p99; the FR-061 durable counts (COMPLETED, background completions, remaining difference, parked events = 0); invariant results; the comparison with `load-client/reports/microservices-architecture/` and with CP0, explaining any change; and links to the diagram.
- [X] T169 [US8] Pass the US8 gate `PARITY_GATE=006:US8 ./run-tests.sh architecture`; fill `Verified at`.

**Checkpoint**: The graded deliverables are committed and genuine.

---

## Phase 11: Polish & Cross-Cutting Concerns

**Purpose**: Final parity, full validation, and documentation consistency.

- [X] T170 Run `PARITY_GATE=006:final ./run-tests.sh architecture` and resolve every remaining `PENDING` row or missing `Verified at` in the 006 section of `server/tests/COVERAGE-LEDGER.md`; confirm no deleted test lacks a `MIGRATED` or `SUPERSEDED` row with its requirement (FR-065).
- [X] T171 Run the release suite: `./build-tests.sh` then `KAFKA_TEST_BOOTSTRAP=127.0.0.1:19092 KAFKA_TEST_BROKER_CONTROL=1 STRICT_LIVE_GATES=1 SERVER_BASE_URL=http://localhost:8080 NGINX_BIN=<nginx.exe> ./run-tests.sh` from `server/`, and save the list of passing classes in `load-client/reports/event-based-architecture/final-gate-tests.txt`, with the direct restoration results listed separately (SC-016).
- [X] T172 Run every quickstart section ([quickstart.md](quickstart.md) §1–7) on a fresh baseline and record each scenario A–H outcome, the build and baseline output, and the diagram check in `load-client/reports/event-based-architecture/quickstart-validation.md`.
- [X] T173 [P] Constitution compliance review against v6.0.0 Principles I–IX and the plan's Constitution Check table: confirm each PASS still holds in the code; record any implementation refinement (for example Implementation Rule 5's shared JDBC adapters, the `events`/`messaging` package split, CP3 adoptions) in a short "Implementation notes" section at the end of `specs/006-event-based-architecture/plan.md`.
- [X] T174 [P] Verify every message type, topic, group, and configuration variable named in code matches [event-catalog.md](contracts/event-catalog.md) and [broker-and-deployment.md](contracts/broker-and-deployment.md), and that `db/validate-invariants.ps1`, `deploy/kafka/status.ps1`, and the status lines report every FR-046 field.

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: T001 and T002 come before any code change. T003 → T004 → T006 (the boundary test must accept the Kafka jar before the first build that produces it). T010 → T011 → T013.
- **Foundational (Phase 2)**: depends on Setup. Blocks every user story.
- **US1 (Phase 3)**: depends on Foundational.
- **US3 (Phase 4)**: depends on US1 (it extends the outcome handler, recovery worker, and Inventory consumer).
- **US4 (Phase 5)**: depends on Foundational; scheduled after US3 so Inventory's gRPC server can be removed whole in T111 (its checkout RPCs are dead after US1, its restoration RPC after US3).
- **US5 (Phase 6)**: depends on Foundational; scheduled after US4 for CP2 and to keep one migration in flight at a time.
- **US2 (Phase 7)**: depends on US5 (scans must flow through the broker for scenario 3).
- **US6 (Phase 8)** and **US7 (Phase 9)**: depend on US1–US5 and US2 (every gRPC path must be gone). US6 and US7 can run in parallel; T153–T156 touch build files only US7 owns.
- **US8 (Phase 10)**: depends on all other stories; T163 → T164 → T165 → T166 → T167 → T168.
- **Polish (Phase 11)**: depends on US8.

### Story completion order

Foundational → US1 (MVP, CP1) → US3 → US4 → US5 (CP2) → US2 → US6 ∥ US7 → US8 (CP3, CP4) → Polish.

### Within each user story

- Test tasks first; they must fail before implementation.
- Schema and store changes before business handlers; handlers before `messaging` adapters; adapters before `*Main` wiring.
- Ledger rows updated before any legacy test is deleted; the story gate last.

### Parallel opportunities

- Setup: T005, T007, T008, T009, T010, T012, T013 after their prerequisites.
- Foundational: all test tasks T015–T022 together; then T024 ∥ T026, T030 ∥ T031 ∥ T032, T035 ∥ T036 ∥ T037 ∥ T038, T042 ∥ T044.
- Each story's `[P]` test tasks together; `[P]` implementation tasks touch distinct files (for example T060 ∥ T061, T062 ∥ T063, T070 ∥ T071 in US1).
- US6 and US7 phases in parallel once US2 is done.

---

## Parallel Example: Foundational

```bash
Task: "Create server/tests/unit/common/EnvelopeCodecTest.java"
Task: "Create server/tests/unit/common/OutboxDispatcherTest.java"
Task: "Create server/tests/unit/common/ConsumerLoopTest.java"
Task: "Create server/tests/messaging/EventSchemaCompatibilityTest.java and baseline"
Task: "Create server/tests/database/JdbcOutboxStoreTest.java"
Task: "Create server/tests/database/JdbcDeadLetterStoreTest.java"
Task: "Create server/tests/messaging/KafkaTransportLiveTest.java"
```

## Parallel Example: User Story 1

```bash
# Tests first:
Task: "Create server/tests/unit/transaction/SettlementRegistryResponderTest.java"
Task: "Create server/tests/unit/transaction/CheckoutOutcomeHandlerTest.java"
Task: "Create server/tests/unit/inventory/CheckoutRequestHandlerTest.java"
Task: "Create server/tests/messaging/CheckoutEventContractTest.java"
# Then independent implementation files:
Task: "Add request_event_id and adapt JdbcCheckoutAttemptStore (Transaction)"
Task: "Add outcome_event_id/restoration_event_id and adapt JdbcCheckoutOperationStore (Inventory)"
Task: "Create server/transaction/src/transaction/CheckoutEvents.java"
Task: "Create server/inventory/src/inventory/StockOutcomeEvents.java"
```

## Parallel Example: User Story 3

```bash
Task: "Create server/tests/integration/CheckoutEventFaultTest.java"
Task: "Create server/tests/integration/CheckoutMessageFaultTest.java"
Task: "Create server/tests/messaging/BrokerRestartCheckoutTest.java"
Task: "Create server/tests/messaging/RestorationEventDirectTest.java"
```

## Parallel Example: User Stories 4 and 5 tests

```bash
Task: "Create server/tests/unit/transaction/CatalogEventHandlerTest.java"
Task: "Create server/tests/integration/CatalogReplicationTest.java"
Task: "Create server/tests/messaging/ScanEventContractTest.java"
Task: "Create server/tests/integration/AnalyticsOffsetAuthorityTest.java"
```

## Parallel Example: User Stories 6 and 7

```bash
Task: "Create server/tests/integration/BrokerConditionErrorMappingTest.java"
Task: "Finalize server/tests/architecture/MicroserviceBoundaryTest.java"
Task: "Create server/tests/architecture/OutboxOnlyPublishTest.java"
Task: "Adapt server/tests/integration/DatabaseIsolationTest.java"
```

---

## Implementation Strategy

### MVP first: checkout over events

1. Phase 1 Setup and Phase 2 Foundational.
2. Phase 3 (US1): checkout over events with Inventory and Transaction exchanging only domain events.
3. **Stop and validate**: US1 gate (T077), quickstart scenario A (T078), and CP1 (T079).

### Incremental delivery

1. US3 makes the Saga fault tolerant; the system is releasable with gRPC still carrying catalog and scans.
2. US4 moves the catalog and removes Inventory's gRPC surface.
3. US5 moves scans and removes Analytics' gRPC surface (CP2).
4. US2 proves scan independence; US6 and US7 prove the contract and independent operation and remove the last gRPC artifacts.
5. US8 produces the official evidence and diagram (CP3, CP4).

### Performance discipline

- Commits on the critical path stay at N + 4 per basket; statement budgets enforce T1, Inventory apply, and outcome handling (T056).
- Each checkpoint reports client-observed `totalTransactions` against CP0. A drop of more than 5% from the previous checkpoint is investigated before continuing; no improvement is claimed from latency or statement counts alone.
- Deferred options (D17) are adopted only when their measured entry criteria are met (T131, T163).

### Test coverage discipline (FR-065)

- No assertion is weakened. Every removed or replaced test has a ledger row with its replacement and requirement before deletion.
- `MicroserviceBoundaryTest` changes in two steps: allow Kafka in adapters (T004), then forbid every synchronous service-to-service mechanism (T146).

---

## Notes

- `[P]` tasks touch different files and depend on no incomplete task.
- `[Story]` labels map tasks to spec user stories for traceability.
- Commit after each task or logical group; never commit while a benchmark runs on this machine.
- Stop at any checkpoint to validate a story on its own.
