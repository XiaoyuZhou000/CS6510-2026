# Research: Event-Based Self-Checkout Microservices

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Constitution**: v6.0.0

Each decision records what was chosen, why, and what else was considered. Together they resolve every unknown in the plan's Technical Context and every item in the spec's *Planning Obligations*. Decision numbers D1–D17 belong to this feature. References written as "005 R*n*" point to `specs/005-microservices-architecture/research.md`, whose decisions stay in force unless a decision below replaces them.

**Starting point**: the feature 005 implementation at commit `8891c57`. It has three services with their own schemas, gRPC calls from Transaction to Inventory (catalog and `ApplyCheckout`) and to Analytics (`DeliverScanEvents`), an `accepted_scan` outbox, a Transaction-orchestrated Saga with forward recovery, and in-memory projections guarded by durable state.

**Environment facts** that constrain the decisions:
- Windows 10 with 24 logical CPUs and 32 GB of RAM.
- Docker Desktop 26.1 is installed but cannot run: the Windows hypervisor stays off because it conflicts with a game used on this shared machine, so WSL 2 fails to start (`HCS_E_HYPERV_NOT_INSTALLED`, 2026-10-29). Erlang is not installed.
- JDK 21 and Maven 3.9.9 are installed.
- MySQL 8.4 runs on port 3307, where each durable commit costs about 7.5 ms (005 R18).
- The TCP dynamic port range is 1024–15000, so ports above 15000 never collide with outbound connections.

---

## D1. Message broker: Apache Kafka 4.1.0, one KRaft node run natively on Windows

**Decision**: Use Apache Kafka 4.1.0 as the only broker. It runs as a single combined broker and controller in KRaft mode, from the official binary release (`kafka_2.13-4.1.0.tgz`, unpacked anywhere and named by `KAFKA_HOME`), as a plain JVM on the host, started by `deploy/kafka/start-broker.ps1` with the committed configuration `deploy/kafka/server.properties`. The client listener is `127.0.0.1:19092`, the controller listener `127.0.0.1:19093`, and data lives in `KAFKA_DATA_DIR` (default `<KAFKA_HOME>\cs6510-data`).

*Amended 2026-10-29*: the first version of this decision ran the `apache/kafka:4.1.0` image in Docker. Docker cannot run on the benchmark machine (environment facts above), so the broker moved to the host. The topics, the clients, and every guarantee below are unchanged; only deployment, stop semantics, and the baseline reset differ.

| Setting | Value | Why |
|---|---|---|
| Replication factor, `min.insync.replicas` | 1, 1 | Single node (spec Assumptions) |
| `auto.create.topics.enable` | `false` | Topics exist only as documented in the [event catalog](contracts/event-catalog.md) |
| `unclean.leader.election.enable` | `false` | No acknowledged write is discarded |
| `log.segment.bytes`, `log.roll.ms`, `offsets.topic.segment.bytes` | 1 GiB, `Long.MAX_VALUE`, 1 GiB; every topic also pins `segment.bytes` and `segment.ms` | Windows: see below. Segments never roll, so retention and compaction never delete or rewrite a file. |
| `delete.topic.enable` | `false` | Windows: a topic deletion takes the broker down (below), so it is refused |
| `log.flush.interval.messages` | Broker default (the OS page cache), with `KAFKA_FLUSH_EVERY_MESSAGE=true` as a measured option (D15) | See durability below |
| Producer | `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `delivery.timeout.ms=30000`, `max.block.ms=1000` | A confirmation means the broker has the record in its log. Idempotence keeps per-partition order across internal retries. A request thread never blocks on a missing broker. |
| Consumer | `enable.auto.commit=false`, `isolation.level=read_uncommitted` (no Kafka transactions), `auto.offset.reset=earliest` | Offsets are committed only after local commits (D6) |

**Windows constraints**, measured in a spike on 2026-10-29:
- Kafka cannot rename or delete a log file that it still has open. Deleting a topic renames its open partition directory, fails with `AccessDeniedException`, and takes the whole log directory offline, which stops the broker (the KAFKA-1194 family of issues). Segment deletion by retention and segment rewriting by compaction fail the same way.
- Producing, consuming, committed offsets, a compacted topic's writes, and recovery after a forced kill all work.
- The Windows `.bat` wrappers fail with "input line too long" while building the classpath, so the scripts run the tools as their Java main classes.

So segments never roll and topics are never deleted. Retention and compaction therefore never act: every topic keeps every record until the baseline reset (D13), which stops the broker and replaces its log directory. The catalog topic keeps `cleanup.policy=compact` to document its last-value-per-SKU meaning; a consumer that replays it from offset 0 (D9) obtains the same latest value per key whether or not compaction has run. Its size grows by one catalog publication (about 2,000 records) per Inventory start, which stays small between resets. Each benchmark run starts from a reset, so a 1 GiB segment never fills.

**Durability (FR-007)**: a confirmed record is in the broker's log before `acks=all` returns. A restart or crash of the Kafka process keeps the log, because the written pages belong to the operating system's file cache, which outlives the process. On Windows the broker has no clean-stop signal, so every stop is a forced kill, and broker-restart tests stop and start the process (D14). Losing the host before the file cache is flushed is outside the constitution's broker-restart guarantee. That residual risk is documented, and `KAFKA_FLUSH_EVERY_MESSAGE=true` (`log.flush.interval.messages=1`) removes it if it is chosen at checkpoint CP3. Unconfirmed records are never lost, because each one stays unpublished in its producer's outbox (D5).

**Rationale**:
- **Ordering**: partitioning by key gives exactly the per-transaction ordering the Saga needs while consumers work in parallel (FR-012).
- **Catalog**: a replayable log lets a new or reset consumer rebuild the full catalog by reading from the beginning, with no request event and no synchronous call (FR-018, D9).
- **Progress**: consumers control their offsets, which matches Analytics' requirement to keep progress in its own database (FR-049), with broker offsets only as an optimization.
- **Fit**: it is the most widely used event-streaming broker, which suits an assignment about going event-based. The Java client is one Maven dependency, and the broker is one unpacked archive run on the JDK already installed.

**Alternatives considered**:
- **Kafka in Docker** (the first version of this decision): rejected after it was planned, because Docker Desktop needs the hypervisor, which must stay off on this machine.
- **Kafka with normal retention and compaction on Windows**: rejected; the first segment deletion or compaction would take the broker down.
- **RabbitMQ 4 (quorum queues)**: strong durability, with an fsync before each confirm, and built-in dead-lettering. Rejected because it needs Erlang (or Docker, which cannot run here), and per-key ordering with competing consumers needs a consistent-hash exchange or single-active-consumer queues. It also has no log to replay, so the catalog would need a snapshot-request event and a snapshot responder.
- **ActiveMQ Artemis 2.x**: pure Java, so it runs natively on Windows, with dead-letter addresses and message groups for per-key ordering. Rejected because it has no replayable log (the catalog would need last-value queues or request and response events), and a durable send waits for a journal sync on this disk, which adds a commit-sized cost to every hop.
- **A second broker, or a broker plus a cache**: forbidden by Principle VIII.

## D2. Build and dependencies: the Maven module becomes event contracts plus the Kafka client

**Decision**:
- **Module**: keep the single Maven module `server/contracts`.
  - Replace the gRPC `.proto` files with event schemas under `server/contracts/src/main/proto/selfcheckout/events/v1/`.
  - Drop the gRPC BOM, the gRPC dependencies, and the `protoc-gen-grpc-java` plugin goal.
  - Pin `protobuf-java` 3.25.9, which is the version already in use, so generated message classes stay stable, together with `org.apache.kafka:kafka-clients:4.1.0` and `org.slf4j:slf4j-simple:2.0.16`, with logging set to WARN.
- **Outputs**: still `out/contracts.jar`. Runtime dependencies are copied to `lib/messaging/`, which is git-ignored and replaces `lib/grpc/`.
- **Scripts**: `build.sh` and the `run-*.sh` classpaths switch from `lib/grpc/*` to `lib/messaging/*`. `build.sh` deletes `lib/grpc/`, so no gRPC jar remains on any classpath after migration.

**Rationale**: it reuses the existing Maven-for-generation and javac-for-services split (005 R1). Protocol Buffers already provides version-controlled schemas with known compatibility rules, which Principle III requires.

**Alternatives considered**:
- **JSON with JSON Schema**: needs a validator dependency and hand-written mapping, and its compatibility checks are weaker.
- **Apache Avro with a schema registry**: the registry is extra infrastructure, which Principle VIII forbids.

## D3. Event envelope, identities, and schema evolution

**Decision**: every Kafka record value is a serialized `selfcheckout.events.v1.EventEnvelope` ([contracts/proto/envelope.proto](contracts/proto/envelope.proto)). The record key is the envelope's `routing_key`.

| Envelope field | Content |
|---|---|
| `event_id` | Globally unique. For scans it is derived deterministically as `scan:<transaction_id>:<scan_sequence>`. For outbox events it is a UUID stored in the outbox row. Re-sending a recorded event reuses its `event_id` (D5). |
| `event_type` | For example `CheckoutRequested`. One owning producer per type (FR-009). |
| `schema_version` | 1 for every type in this feature |
| `routing_key` | Transaction ID for checkout, outcome, and scan events; SKU or `__snapshot__` for catalog events |
| `occurred_at_ms` | Producer's clock when the fact was recorded (informational only) |
| `correlation_id` | Transaction ID, or catalog version for catalog events |
| `producer` | `transaction` or `inventory` |
| `payload` | The serialized type-specific message |

**Evolution (FR-014)**:
- Within v1, a change may only add optional fields with new tag numbers. Removed fields become `reserved`.
- An incompatible change requires a new type or a `v2` package.
- `tests/messaging/EventSchemaCompatibilityTest` compares the compiled descriptors with committed baselines in `tests/messaging/baseline/`, reusing the approach of 005's `ProtoCompatibilityTest`. A consumer meeting an unknown `schema_version` or `event_type` parks the record (D6).

## D4. Topics, keys, partitions, and consumer groups

**Decision**: there are four topics, created by `deploy/kafka/create-topics.ps1` and never auto-created. The full catalog of types, producers, and consumers is in [contracts/event-catalog.md](contracts/event-catalog.md).

| Topic | Partitions | Cleanup | Key | Producer | Consumer |
|---|---:|---|---|---|---|
| `selfcheckout.catalog.v1` | 1 | `compact`, infinite retention | SKU, or `__snapshot__` | Inventory | Transaction, by manual assignment from offset 0 at every start |
| `selfcheckout.checkout.v1` | 16 | `delete`, 7 days | Transaction ID | Transaction | Inventory, group `inventory-checkout` |
| `selfcheckout.checkout-outcome.v1` | 16 | `delete`, 7 days | Transaction ID | Inventory | Transaction, group `transaction-checkout-outcome` |
| `selfcheckout.scan-accepted.v1` | 1 | `delete`, 7 days | Transaction ID | Transaction | Analytics, group `analytics-scan-ingest` |

**Rationale**:
- **Checkout topics**: all of a transaction's checkout and restoration events share one partition, so a delayed `CheckoutRequested` and a later `StockRestorationRequested` for the same attempt keep their order (FR-012). Sixteen partitions allow up to 16 parallel consumers per group.
- **Scan topic**: one partition gives Analytics a single total order. That order is the documented ordering rule (D10). At about 2,300 scans/s under stress, it is far below one partition's capacity.
- **Catalog topic**: one partition keeps the snapshot marker after its items (D9).

**Alternatives considered**:
- **One topic per event type**: it would split a transaction's checkout and restoration events across partitions and lose their relative order.
- **More scan partitions**: Analytics serializes ingestion on its position lock anyway (005 R11), so they add nothing.

## D5. Transactional outbox and publication

**Decision**: every published event is first written in its producer's database, in the same local transaction as the state change that causes it (FR-010).

| Service | Outbox | Written by | Published by |
|---|---|---|---|
| Transaction | `accepted_scan` (unchanged role; `delivered_at` renamed `published_at`) | The Scan transaction | `ScanEventRelay` (D10) |
| Transaction | `outbox_event` | T1 (`CheckoutRequested`) | `OutboxDispatcher` |
| Inventory | `outbox_event` | The apply or restore transaction (outcomes) and the catalog publication transaction (D9) | `OutboxDispatcher` |

**`OutboxDispatcher`** (in `common/events`, Kafka-free behind the `RecordSender` port, one per service):
1. **Immediate path**: after a local commit, the committing thread hands the new event IDs to the dispatcher's in-memory queue and continues. It never waits for the broker. The dispatcher thread sends them with the producer.
2. **Confirmation**: when a send's future completes successfully, the event ID joins a marking batch. A marker writes `UPDATE outbox_event SET published_at=? WHERE event_id IN (…)` and commits, once per 20 ms or 200 IDs. Marking is off the critical path and costs one commit per batch.
3. **Sweep**: every `OUTBOX_SWEEP_MS` (1,000 ms), one indexed query selects up to 500 rows where `published_at IS NULL AND created_at < now − 1 s`. That covers crashes, broker outages, and failed sends. Rows are re-sent in `outbox_seq` order with backoff from 100 ms to 5 s.
4. **Re-send of a recorded event**: recovery (D7) and duplicate handling (D8) re-send an existing outbox row, identified by its `event_id` and carrying its stored payload, through the same producer. They never create a new event for an old fact.

Outbox rows are retained until the next reset. A stress run writes about 25,000 outbox rows, so no purge is needed. A purge of published rows older than 7 days is listed as an option.

**Ordering**: the dispatcher sends one partition key's events in commit order, because one dispatcher thread drains the queue in order and the sweep orders by `outbox_seq`. Kafka's idempotent producer keeps that order on the partition.

**Rationale**: the immediate path removes polling latency from Complete, which a 50 ms poll would add twice per basket. The sweep guarantees eventual publication without relying on memory. Batched marking keeps the commits on the critical path at 005's N + 4 per basket (D15).

**Alternatives considered**:
- **A polling-only relay**: adds up to two poll intervals of latency to every checkout.
- **Kafka transactions with change data capture (Debezium)**: needs extra infrastructure (Kafka Connect) and the MySQL binlog, which Principle VIII forbids.
- **Publishing before or without the outbox**: prohibited by Principle VII.

## D6. Consumers: acknowledge after commit, idempotency, and parking

**Decision**: a shared `ConsumerLoop` in `common/events` (Kafka-free; it reads through the `RecordStream` port, which `common/messaging` implements with `KafkaConsumer`) runs each consumer group with `N` threads (one `KafkaConsumer` each). Handlers live in each service's business layer behind an `EventHandler` port.

| Rule | Value |
|---|---|
| Acknowledgement | A record's offset is committed (`commitAsync`, then `commitSync` on revocation and shutdown) only after the handler's local transaction has committed (FR-011) |
| Idempotency keys | Inventory: `checkout_operation` primary key `(transaction_id, attempt_number)`. Transaction outcome handler: the attempt's state and guarded transitions. Analytics: unique `(source_transaction_id, source_scan_sequence)`. Transaction catalog handler: `(sku, catalog_version)`. |
| Transient failures (database unavailable, deadlock after internal retries, ambiguous commit: every `StoreFailure`, plus `TransientEventFailure`) | Not counted as poison. The partition is paused and the same record is retried with backoff from 100 ms to 5 s, indefinitely, with no offset committed. Unrelated partitions keep flowing on other threads. |
| Permanent failures (undecodable envelope, unknown `event_type` or `schema_version`, payload validation failure, integrity fault such as a reused attempt key with different lines, and any other unclassified exception, parked as `HANDLER_ERROR`) | Retried `CONSUMER_MAX_ATTEMPTS` (5) times, 200 ms apart, then parked |
| Stall alarm | A record still in transient retry after 60 s is logged with its topic, partition, offset, and age once per status interval, so a misclassified fault is visible rather than silent |
| Parking | One local transaction INSERTs a `dead_letter_event` row (topic, partition, offset, `event_id`, type, reason, payload bytes, first and last seen, count) and then commits the offset. An event ID parked again increments `count`. Parked records are visible in the status line and status query (FR-013, FR-046). |
| Non-blocking | A poison record holds its partition for at most about 1 s (five attempts) before parking. Transient waits hold only that partition. |

**Rationale**: business keys, not message IDs, are the idempotency keys wherever a duplicate must produce the original outcome again, as Inventory must (D8). A "processed event ID" table would add one write per event without adding safety. A database table serves as the dead-letter destination: it is the constitution's "equivalent parked state", it is queryable with the service's own credentials, and it survives a broker reset.

**Alternatives considered**:
- **Dead-letter topics**: they add topics and tooling, and a parked record would vanish with a broker reset.
- **Kafka exactly-once transactions**: they do not cover the MySQL writes, so idempotent consumers are still needed.

## D7. Choreographed checkout Saga over domain events

**Decision**: the Saga is choreographed (spec clarification, FR-031). Transaction Service still owns its state durably in `checkout_attempt` (005 R5). The event sequence is:

```text
Transaction T1 (guarded UPDATE + INSERT attempt + INSERT outbox CheckoutRequested, one commit)
  → CheckoutRequested ─▶ Inventory: apply atomically + record operation + INSERT outbox outcome, one commit
  → StockDeducted | StockRejected | StockDeductionRefused ─▶ Transaction outcome handler:
        T3 (COMPLETED) or reopen (REJECTED), one commit, then complete the settlement future
```

- **Complete request**:
  1. Run the 005 pre-checks and T1, with one more INSERT into `outbox_event` in the T1 transaction.
  2. After the commit, signal the dispatcher.
  3. Wait on the attempt's `SettlementRegistry` future for the rest of the 8 s budget (`CHECKOUT_RESPONSE_BUDGET_MS`), holding no connection or lock.

  The request thread no longer calls Inventory or retries anything. The 2 s RPC deadline and the in-request retry delays are removed.
- **Outcome handler** (`CheckoutOutcomeHandler`, Transaction business layer): runs the 005 T3 or reopen guarded transition under the transaction's stripe lock, updates the cache, and completes the future.
  - An outcome for an attempt that is already settled, or for an older attempt, is a no-op.
  - An outcome for an unknown attempt, or a deduction for a transaction that is not pending that attempt, is an integrity fault: it is parked and alarmed (FR-038).
  - A `StockRestored` result reopens a pending attempt with reason `STOCK_RESTORED`. A `StockRestored` result for a COMPLETED transaction is parked as an integrity fault, because it must never happen.
- **Client-outcome claim**: each live request registers as the attempt's responder in the `SettlementRegistry` entry, with state WAITING.
  - The handler tries to move the responder from WAITING to ANSWERING (compare and set) before its T3 or reopen commit. If that succeeds, it writes `client_outcome = SUCCESS` or `REJECTED` inside the same UPDATE, and the request answers from the settlement even if its budget runs out during that commit.
  - When the budget ends, the request tries to move WAITING to TIMED_OUT. If that succeeds, it commits `client_outcome = PENDING_ERROR` and returns 500, as in 005. If it fails, the handler already owns the answer.

  This keeps the R9 counts exact with no extra commit on the success path.
- **Republish schedule** (FR-037):
  - T1 sets `next_retry_at = created_at + CHECKOUT_REPUBLISH_AFTER_MS` (2,000 ms).
  - The existing `CheckoutRecoveryWorker` and `RecoverySchedule` now re-send the attempt's recorded `CheckoutRequested` (D5) instead of calling `ApplyCheckout`. Backoff runs from 2 s to a 5 s cap, indefinitely, with `retry_count` and `last_error` updated.
  - The startup scan and the 30 s safety scan are unchanged.
  - A worker never settles an attempt itself. Settlement happens only through an outcome event.
- **Stuck alarm**: an attempt still PENDING after `CHECKOUT_STUCK_ALARM_SECONDS` (60) is reported as alarmed, with its age, in the status line. That covers outcomes parked in either service, without guessing an outcome (FR-038, Principle VI).
- **Step and compensation table**: unchanged from 005. T1 is compensated by reopening. Inventory's deduction is the pivot. T3 is retried, here through redelivery and republish, until it commits. Restoration stays reserved. See [contracts/checkout-saga.md](contracts/checkout-saga.md).

**Rationale**: with only two participants, choreography carries the same information as orchestration, and the existing durable state machine, recovery worker, registry, and fault matrix carry over with the RPC replaced by an outbox event and the reply replaced by a consumed event. The responder claim is the smallest change that keeps background-completion accounting exact once settlement moves from the request thread to a consumer thread.

**Alternatives considered**:
- **Command and reply messages (orchestration)**: excluded by the clarification.
- **A per-request reply topic**: needs reply routing that the clarification forbids, and it is unnecessary with one process.
- **Recording the client outcome in a second commit after T3**: adds 7.5 ms to every successful Complete.

## D8. Inventory checkout handling and the reserved restoration

**Decision**:
- **`CheckoutRequested`**: `CheckoutRequestHandler` calls the existing `CheckoutStockService` apply logic (005 R8). The local transaction is the locking SELECT in SKU order, then the INSERT of `checkout_operation`, then the CASE UPDATE if applied, then **one INSERT into `outbox_event` with the outcome event**, then the commit: four statements and a commit. The new column `checkout_operation.outcome_event_id` links the operation to its outcome event.
- **Duplicate or redelivered request** (the primary key already exists): roll back, read the recorded operation, and re-send its recorded outcome event by `outcome_event_id` (D5). No stock changes and no row is written.
  - If the stored `lines` differ, the request is an integrity fault and is parked (D6), and nothing is answered. Transaction's stuck alarm surfaces it.
  - A RESTORED or BLOCKED record answers `StockDeductionRefused`, also through its recorded outcome event.
- **Reserved restoration**: `StockRestorationRequested`, keyed by transaction ID on the checkout topic, is owned by Transaction Service. Feature 005 R8's decision stands: **there is no operational use**. No production code path produces the event. Direct tests in `tests/messaging/RestorationEventDirectTest` record it in Transaction's `outbox_event` and send it through Transaction's `OutboxDispatcher`, as the owning producer would, so even this test-only producer obeys the outbox rule (FR-010).
  - Inventory's `StockRestorationHandler` applies the existing idempotent restore, which yields RESTORED, BLOCKED, or "already" or "nothing to restore", and writes one outcome event (`StockRestored`, `RestorationNotNeeded`) in the same transaction.
  - Transaction's outcome handler reopens a pending attempt on `StockRestored` (D7). Results are reported separately from the end-to-end Saga results (FR-039, SC-016).
- **Contention and transient faults**: they keep 005's internal deadlock retry. A fault that persists is transient (D6), never `INSUFFICIENT_STOCK` (FR-033).

## D9. Catalog replication through a compacted topic

**Decision**:
- **Publication**: at every Inventory startup, `CatalogPublisher` runs one local transaction that writes 2,000 `CatalogItemPublished` outbox events (key SKU; payload SKU, name, price, `catalog_version`) followed by one `CatalogSnapshotPublished` event (key `__snapshot__`; payload `catalog_version` and `product_count`), and records `catalog_meta.published_at`. The catalog changes only through reset and reseed, which happen while Inventory is stopped, so publishing at startup covers every change. It also makes the topic self-healing if it is ever recreated: restarting Inventory republishes. The cost is one transaction of about 2,001 batched rows at startup.
- **Consumption**: Transaction's `CatalogConsumer` assigns partition 0 manually and reads from offset 0 at every start, then keeps tailing.
  - Items are buffered by version.
  - When a `CatalogSnapshotPublished` marker arrives whose version differs from the replica's version and whose count of distinct buffered SKUs for that version equals `product_count` (records repeated by several Inventory startups before compaction are counted once), the replica is replaced atomically in one local transaction (005 R4's `product_replica` and `product_replica_meta`), and the in-memory snapshot is swapped.
  - A marker for the current version is a no-op.
  - Records from older versions that compaction keeps under SKUs no longer in the catalog are ignored, because their version never matches a complete marker.
- **Ordering and staleness**: one partition keeps the marker behind its items, and every reseed has a new version UUID, so a late older update can never overwrite a newer one (FR-012). Staleness is bounded by the time from Inventory's startup to the consumer reading the marker, under 5 s when healthy. While the broker or Inventory is down, the durable replica keeps serving (FR-017).
- **Readiness**: the benchmark harness waits until Transaction's status reports `replica_version` equal to Inventory's `catalog_version` before starting the Load Client (SC-009).
- **Analytics names**: they keep traveling inside `ScanAccepted` (`product_name`), so Analytics needs no catalog consumer (005 R12, FR-019).

**Alternatives considered**:
- **A `CatalogSnapshotRequested` event answered by Inventory**: adds a request type, which compaction makes unnecessary.
- **Tombstones for removed SKUs**: Inventory cannot know the previous catalog after a reset, and version matching makes tombstones unnecessary.
- **Committed consumer offsets for the catalog**: replaying 2,001 small records at startup is cheaper than tracking them.

## D10. Scan delivery to Analytics through the broker

**Decision**:
- **Write**: the Scan transaction is unchanged: one guarded UPDATE plus one INSERT into `accepted_scan`, with no broker involvement (FR-015).
- **Relay**: `ScanEventRelay` keeps its loop: up to 500 rows with `published_at IS NULL` in `scan_id` order, and a 50 ms idle poll. Its `ScanEventSink` is now `KafkaScanEventSink`, which sends the batch with `linger.ms=5`, waits for every confirmation, and then returns `Delivered`. The relay then batch-UPDATEs `published_at`. Any send failure returns `Uncertain`, and the same rows are re-sent after backoff. `Delivered` now means confirmed by the broker, not received by Analytics.
- **Analytics consumer**: `ScanAcceptedConsumer`, with one thread and group `analytics-scan-ingest`, polls up to 500 records. It decodes them and calls `ScanIngestionService`, whose ingestion transaction (005 R11) also UPSERTs `consumer_offset(topic, partition, next_offset)`. After the commit it calls `commitAsync`.
  - **Startup**: on partition assignment it seeks to the stored `next_offset`, so the database is the authority (FR-049).
  - **Offsets out of range**: if the stored offset lies beyond the log's end, for example because the topic was recreated, it logs an alarm and seeks to the earliest offset. Deduplication by `(source_transaction_id, source_scan_sequence)` absorbs any overlap.
- **Ordering rule** (FR-050): positions follow the single partition's offset order, which is Transaction's publication order, within ingestion batches. Late or duplicated records receive the next position or are filtered out, as in 005 R11.
- **Visibility** (FR-055, SC-008): under healthy load the target is under 5 s at p99 from a boundary scan's commit to its window's publication. The parts are the relay poll (≤ 50 ms), the broker hop, the consumer fetch, and the existing poller (100 ms). After load stops, the highest boundary is published within 60 s.

## D11. Bounded waiting and multiple Transaction processes

**Decision**: the waiting rules (005 R7) are unchanged.
- **One process**: the outcome handler completes the registry future in the same process, so the waiting request answers at once (FR-044).
- **Several processes**: outcome events for a transaction may be consumed by another process, which settles the attempt durably. The waiter's existing database backstop (`CHECKOUT_WAIT_BACKSTOP_MS` = 1,000 ms) usually sees the settlement before the budget ends. Otherwise the waiter returns 500 and the attempt has already settled. No reply routing is added, following the clarification.
- **Guarantees**: every safety guarantee rests on database guards and Inventory's idempotency, exactly as in 005. `MultiInstanceTransactionTest` keeps running two processes against one schema, now with the outcome group split between them.

## D12. Failure behavior and error mapping

**Decision**: there are no new external error codes (FR-024).
- **Broker down**:
  - Start, Scan, and Lookup are unaffected, because they never touch the producer or a consumer.
  - Complete commits T1, then its dispatcher send fails within `max.block.ms`, the row stays unpublished, and the request waits out its budget and returns 500 `INTERNAL_ERROR`. The sweep publishes the row when the broker returns.
  - `/items`, `/inventory/low-stock`, and `/analytics/popular-items` read their own databases and keep working.
- **Mapping**: `TransactionFailure` mapping is unchanged. The removed `DependencyUnavailableFailure` paths for catalog RPCs disappear. "No replica" stays `ProductCatalogUnavailableFailure`, which maps to 500.
- **Consumers while their database is down**: they stop committing offsets (D6), so events wait on the broker.

## D13. Reproducible baseline including the broker

**Decision**: one procedure, `db/reset-baseline.ps1`, wrapped by the benchmark harness:
1. Check that all three services are stopped. The script refuses to run otherwise, because a running service could publish into a fresh topic from a stale outbox.
2. Run `deploy/kafka/reset-topics.ps1`. A topic cannot be deleted on Windows (D1), so it stops the broker, deletes its log directory (every topic, committed offset, and the cluster metadata), restarts it with the same cluster ID, recreates the four topics, and verifies that every end offset is 0 and that no consumer group remains.
3. Run `db/reset-all.ps1` (005 R13). Transaction's reset empties the replica, outbox, and attempts; Inventory's reset reseeds the catalog with a new `catalog_version` and empties its outbox and operations; Analytics' reset empties events, windows, and `consumer_offset`.
4. Start Inventory (it publishes the catalog, D9), then Transaction, then Analytics. Wait until the replica is ready, all three consumer groups have partitions assigned, and no outbox row is unpublished.

The result matches FR-059: Transaction holds only the replica, and no stale message or outbox row crosses into the run. `db/validate-invariants.ps1` adds checks for zero unpublished outbox rows, zero parked events, and an empty consumer lag at the end of a run.

## D14. Testing strategy

**Decision**:
- **Seams**: business code depends only on the `events` ports `OutboxStore`, `OutboxSignal`, and `EventHandler`, never on the `messaging` package.
- **In-memory bus**: `tests/support/InMemoryEventBus` implements partitions, consumer groups, offset commits, and fault controls: duplicate, drop before consume, delay, reorder across partitions, redeliver after a "lost" acknowledgement, pause (broker down), and kill a consumer between commit and acknowledgement. Saga, catalog, and analytics fault suites run deterministically on it, together with the existing fault hooks (renamed to cover publish and consume points, see [contracts/checkout-saga.md](contracts/checkout-saga.md#test-only-fault-hooks)).
- **Live Kafka**: `tests/messaging/` runs against a real broker at `KAFKA_TEST_BOOTSTRAP` (default `127.0.0.1:19092`, with a per-run topic prefix). It covers producer confirmation, ordering per key, offset handling, parking, schema decoding, and broker restarts. Tests create their prefixed topics through the Kafka `Admin` client and never delete a topic (D1); prefixed topics accumulate until the next baseline reset. A broker restart stops and starts the broker process through `deploy/kafka/stop-broker.ps1` and `start-broker.ps1`, and is gated by `KAFKA_TEST_BROKER_CONTROL=1`. On Windows every stop is a forced kill, so the clean-restart and kill cases of the constitution are the same case, and an outage test keeps the broker stopped for a measured interval before starting it. With `STRICT_LIVE_GATES=1`, which `run-tests.sh` sets for release runs, a missing broker fails the run instead of skipping it, following the existing `SERVER_BASE_URL` pattern.
- **Architecture tests**: `MicroserviceBoundaryTest` no longer forbids Kafka. It now forbids `io.grpc`, `java.net.http.HttpClient`, `java.net.Socket`, `URL.openConnection`, and peer host or port configuration in service source. `org.apache.kafka` imports are allowed only in `common/messaging` and the services' `messaging` adapter packages. Redis remains forbidden. A new `OutboxOnlyPublishTest` checks that no class outside `OutboxDispatcher` and `ScanEventRelay`'s sink calls the producer. `LayerDependencyTest` swaps `rpc` for `messaging`.
- **Coverage ledger**: every test of the removed gRPC surface (`tests/rpc/*`, `GrpcTestSupport`, `FakeInventoryGateway`) is recorded in `server/tests/COVERAGE-LEDGER.md` with its event-based replacement and governing requirement before deletion (FR-065).

## D15. Performance model, settings, and checkpoints

**Cost model**:
- **Commits on the critical path per basket**: N + 4, exactly as in 005: Start, N Scans, T1, Inventory's apply, and T3. The outbox INSERTs ride inside T1 and Inventory's transaction (one extra statement each, no extra commit).
- **Latency added to Complete**: two broker hops, each a produce with `acks=all` to the local broker plus a consumer fetch, about 2–5 ms each. Two commit-path round trips become event handoffs.
- **Off-path commits**: outbox marking, one commit per 20 ms batch per service.

**Settings**:
- **Producers**: `linger.ms=0` for checkout and outcome events, and `5` for scan and catalog batches. Inventory's single `OutboxDispatcher` routes each row to the producer registered for its topic.
- **Consumers**: `fetch.min.bytes=1` and `fetch.max.wait.ms=50`. Inventory and Transaction checkout consumers run `CHECKOUT_CONSUMER_THREADS` (8) each, on 16 partitions.
- **Pools**: unchanged. The connection budget stays 122.

**Measurement**:
- **New `PERF_METRICS` timers**: outbox commit-to-send, send-to-confirm, consume lag (record timestamp to handler start), handler time, Complete's wait time, republishes, parks, and marker batch size.
- **Harness samples**: consumer lag per group, through `deploy/kafka/status.ps1 -Json`. Each call starts three Kafka tool JVMs and takes about 4 s, so lag is sampled every 10 s while the other samples stay at 2 s.

**Checkpoints** (005 R22 method, with the streamlined policy: ±3% counts as equivalent, one screening pair per relevant workload, three confirmation pairs only at ≥ +5%):
- **CP0**: feature 005 at `8891c57`, re-measured on this machine.
- **CP1**: Saga over events.
- **CP2**: full system with catalog and scans over the broker.
- **CP3**: A/B of the measured options: `KAFKA_FLUSH_EVERY_MESSAGE`, consumer threads 4 against 8, scan `linger.ms` 5 against 20, and `OUTBOX_COALESCE_MS`.
- **CP4**: the official default and stress runs, designated in advance, compared with CP0 and with feature 005's reports in `load-client/reports/microservices-architecture/`.

A lower `totalTransactions` than 005 is an acceptable, explained outcome (spec Assumptions). Correctness is never traded for throughput.

## D16. Evidence and architecture diagram locations

**Decision**:
- **Run directories**: `load-client/reports/event-based-architecture/<label>/`, produced by the harness with `evidence.json`, samples, validator output, and a byte-identical copy of the client report, following 005's layout.
- **Official reports**: copied byte-identically to `load-client/reports/event-based-architecture/default-report.json` and `stress-report.json`, so the graded files have obvious names. A `README.md` beside them gives the commands, baselines, FR-061 counts, and the comparison with feature 005's reports.
- **Diagram**: authored as Mermaid in `docs/architecture/event-based-architecture.md`, which GitHub renders in its web view (FR-063), with an exported `event-based-architecture.svg` beside it for viewers outside GitHub. The root `README.md` links to both.
  - The diagram shows the gateway, the three services, three schemas, the Kafka broker, the four topics with their producers and consumers, and the dead-letter tables.
  - `tests/architecture/ArchitectureDiagramConsistencyTest` checks that every topic and consumer group in the event catalog appears in the Mermaid source, and that no service-to-service edge exists outside the broker.

## D17. Deferred options and their entry criteria

| Option | Adopt only if |
|---|---|
| Inventory group commit (several attempts per local transaction) | CP2 shows Inventory's consumer lag above 50 ms at p95 under stress |
| Immediate publish for scans, instead of the relay poll | SC-008 visibility misses its 5 s p99 target |
| Purging published outbox rows | Outbox tables exceed 1 million rows in practice |
| Kafka `log.flush.interval.messages=1` by default | CP3 shows a `totalTransactions` change within ±3% |
| More partitions for scans | Never, under the single ordering rule (D4) |
