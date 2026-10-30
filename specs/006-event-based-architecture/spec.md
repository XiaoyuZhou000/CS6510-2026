# Feature Specification: Event-Based Self-Checkout Microservices with a Message Broker, Broker-Carried Saga, and Event-Driven Replication

**Feature Branch**: `event-based-architecture`

**Created**: 2026-10-29

**Status**: Draft

**Input**: User description: derived from `Event Based Architecture Requirement.txt`: "Implement all communication between microservices using events, through a message broker of your choice (RabbitMQ, Kafka, ActiveMQ, or another). Submit the repository with the two JSON files produced by running the Load Client against the server, one with default parameters and one in stress mode (`java -cp out Main --stations=100 --duration=120`), and an architecture diagram showing the microservices and the event broker."

## Baseline and Sources

- **[C-I]–[C-IX]** refer to the principles of `.specify/memory/constitution.md` v6.0.0, amended on 2026-10-28. That amendment makes broker-carried events the only form of communication between microservices.
- **[Contract]** refers to `spec/self-checkout-openapi.yaml`, which stays authoritative, together with the current HTTP handlers and contract tests that define error details the OpenAPI document does not enumerate.
- **[Preserved]** refers to the feature 005 implementation in `server/`, its automated tests, and specifications `specs/001-*` through `specs/005-*`. `specs/005-microservices-architecture` is historical: where this specification differs from it, this specification governs, and feature 005's documents are not edited.
- **[Client]** refers to the unchanged Load Client in `load-client/src/`.
- **[Prior reports]** refers to the feature 005 benchmark evidence in `load-client/reports/microservices-architecture/`.
- **[Assignment]** refers to `Event Based Architecture Requirement.txt`.

Today, three independently deployable services sit behind one gateway URL, and each owns its own database schema and credentials. They talk to each other through synchronous internal calls:

- Transaction Service keeps a durable replica of the catalog and refreshes it by polling Inventory Service.
- Complete runs a Saga orchestrated by Transaction Service. It calls Inventory Service synchronously to apply each checkout attempt's stock deduction, and a recovery worker repeats that call under the same attempt identifier until the outcome is known.
- An outbox relay in Transaction Service pushes accepted-scan events to Analytics Service through a synchronous call. Analytics Service deduplicates the events, assigns positions, and publishes complete windows. [Preserved]

This feature replaces each of those synchronous interactions with messages that pass through a single message broker, and then removes the synchronous internal interfaces. Every message is first recorded in its producer's database. Every consumer is idempotent. The checkout Saga, catalog replication, and scan delivery all survive broker restarts and redelivery. Nothing a client can observe changes. [C-III][C-IV][C-V][C-VII]

All user stories are required for acceptance. Priorities set delivery order, not optionality.

## Clarifications

### Session 2026-10-29

- Q: Should every message between services be a domain event that states a fact, or may checkout use command-and-reply messages? → A: Domain events only (choreography). Every message states something that already happened, and Transaction Service still durably owns the checkout's Saga state. (FR-006, FR-031)
- Q: If more than one Transaction Service process runs, must a waiting Complete still get its real answer in time? → A: Only with a single process. With several processes, every durable Saga, stock-consistency, idempotency, and recovery guarantee still holds, but outcome events need not be routed to the process holding the waiting request. A request that misses its answer receives `500 INTERNAL_ERROR`, the Saga settles in the background, retries cause no duplicate deduction, and lookup shows the settled status. No reply routing or other infrastructure is added only for this case. (FR-044)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Complete Checkout Through Broker-Carried Messages (Priority: P1)

As a shopper, I start a transaction, scan items, and complete payment, and I receive the same receipt and errors as before. Behind the scenes, Transaction Service and Inventory Service coordinate the checkout only through messages exchanged via the broker. [Contract][C-III][C-V][C-VI]

**Why this priority**: Checkout is the primary business operation, and it is the only synchronous client request whose answer now depends on a round trip of asynchronous messages.

**Independent Test**: With all services and the broker running, seed controlled stock, scan known baskets, and complete them, including concurrently and with insufficient stock. Compare receipts, transaction status, and Inventory's stock with expected values. Confirm from broker and service records that every interaction between the two services was a message and that no synchronous service-to-service call took place.

**Acceptance Scenarios**:

1. **Given** an OPEN transaction with two units of SKU A and one unit of SKU B, **When** completion succeeds, **Then** the response is HTTP 200 with the established receipt fields, each line carries the unit price captured at scan time, Inventory shows A reduced by exactly 2 and B by exactly 1, and lookup shows COMPLETED.
2. **Given** an OPEN basket in which at least one SKU exceeds available stock, **When** completion is requested, **Then** the response is `409 INSUFFICIENT_STOCK` naming a SKU without enough stock, no SKU's stock changes, and the transaction stays OPEN with its basket, so that further scans and a later completion attempt behave as they do today. This is the end-to-end compensation path of the checkout Saga.
3. **Given** two OPEN transactions that each hold the last available unit of a SKU, **When** both complete concurrently, **Then** at most one succeeds, the other receives `409 INSUFFICIENT_STOCK`, and stock never becomes negative.
4. **Given** a completed transaction, **When** completion is requested again, including after restarts of any service or of the broker, **Then** the response is `409 TRANSACTION_NOT_OPEN` and Inventory records no further effect.
5. **Given** an unknown transaction, or an OPEN transaction with an empty basket, **When** completion is requested, **Then** the response is `404 TRANSACTION_NOT_FOUND` or `409 EMPTY_BASKET` respectively, and no checkout message is published.
6. **Given** a SKU's catalog price changes in Inventory Service after a unit was scanned, **When** the transaction completes, **Then** the receipt and totals use the price captured at scan time.
7. **Given** any successful or rejected checkout, **When** the interaction between Transaction and Inventory is inspected, **Then** it consists only of broker messages, each recorded first in its producer's database.

---

### User Story 2 - Scan Durably Without Waiting on the Broker or Any Peer (Priority: P1)

As a station, each scan is validated, priced, and durably recorded by Transaction Service alone. Scanning keeps working while the broker, Inventory Service, or Analytics Service is unavailable, and every accepted scan still reaches Analytics exactly once after they return. [C-II][C-III][C-VI][C-VII]

**Why this priority**: Scan is the highest-frequency operation. Making it wait on a broker publish or a peer would cut throughput and availability.

**Independent Test**: Stop the broker, then start transactions, scan, and look them up. Restart Transaction Service and continue scanning. Start the broker again and verify that stock is unchanged and that every accepted scan reaches Analytics exactly once. Repeat with Inventory and then Analytics stopped instead of the broker.

**Acceptance Scenarios**:

1. **Given** Transaction Service holds product information, **When** a valid scan is accepted, **Then** the response returns the established fields, item count, and running total, and the request neither calls another service nor waits for the broker.
2. **Given** the broker is stopped, **When** stations start transactions, scan known SKUs, and look them up, including after a Transaction Service restart, **Then** all requests succeed with correct names and prices, and the accepted scans remain durably recorded for later publication.
3. **Given** the broker or Analytics Service was unavailable while scans were accepted, **When** both are available again, **Then** each of those scans is counted exactly once.
4. **Given** scans were acknowledged, **When** Transaction Service terminates unexpectedly and restarts, **Then** lookup shows the same status, item count, total, and start time; completion uses every acknowledged unit; and every acknowledged scan is still delivered to Analytics.
5. **Given** any number of accepted scans, **When** stock is inspected, **Then** no scan has changed stock.
6. **Given** a scan is rejected for any reason, **When** state is inspected, **Then** neither the basket nor the record of accepted-scan messages has changed.

---

### User Story 3 - Settle Every Checkout Exactly Once Despite Message and Broker Faults (Priority: P1)

As the store, every checkout interrupted by a timeout, lost or duplicated message, redelivery, crash, or broker restart ends in exactly one settled outcome that agrees with Inventory, with no double deduction, no double restoration, and no manual repair. [C-III][C-V][C-VI]

**Why this priority**: Asynchronous, at-least-once messaging adds new failure modes, namely duplicate delivery, redelivery after a lost acknowledgement, delayed delivery, and broker outages, on top of the partial failures that feature 005 already handles.

**Independent Test**: Inject faults at each checkout step:

- before the stock request is published;
- after it is published but before Inventory consumes it;
- after Inventory commits the deduction but before its outcome is published;
- after the outcome is published but before Transaction Service consumes it;
- after Transaction Service records completion but before the client response.

At each point, crash and restart Transaction Service, Inventory Service, and the broker. Duplicate, delay, and redeliver every checkout message, and keep the broker or Inventory unavailable past the response deadline. Verify each checkout's settled state, the client's responses, and stock reconciliation, and confirm that no timeout or lost message triggered stock restoration. Separately, exercise the reserved stock-restoration operation directly against Inventory Service, with duplicated, repeated, crash-interrupted, and out-of-order restoration messages, and report those results apart from the end-to-end Saga results.

**Acceptance Scenarios**:

1. **Given** Inventory applied a checkout's deduction but its outcome message was lost before publication, **When** Transaction Service publishes the stock request again under the same attempt identifier, **Then** Inventory publishes the recorded outcome again, stock is reduced exactly once, and the transaction becomes COMPLETED without stock restoration.
2. **Given** a stock request or outcome message is delivered more than once, including after a consumer crashed before acknowledging it, **When** it is processed, **Then** it has its business effect at most once and produces the same recorded outcome.
3. **Given** the broker restarts while checkout messages are in flight, **When** it returns, **Then** no acknowledged message is lost, each pending attempt settles automatically, and no stock effect is applied twice.
4. **Given** Transaction Service crashes at any checkout step, **When** it restarts, **Then** it resumes the pending attempt from its durable progress, learns Inventory's actual outcome through messages, and settles the checkout without manual intervention.
5. **Given** a checkout does not settle before its response deadline, for example because the broker or Inventory is unavailable, **When** the deadline passes, **Then** the client receives `500 INTERNAL_ERROR`, never success, within the client's 10-second request timeout. That response does not mean the checkout was cancelled: the attempt stays pending and later settles as COMPLETED if Inventory applied the deduction, or as OPEN with its basket if Inventory definitively rejected it, which lookup then shows.
6. **Given** a checkout completed after its client received `500 INTERNAL_ERROR`, **When** the client repeats Complete, **Then** the response is `409 TRANSACTION_NOT_OPEN`, stock does not change, and lookup shows COMPLETED.
7. **Given** a checkout is pending, **When** a Scan or Complete for the same transaction arrives, **Then** it waits up to the bound and is then applied in serial order: it receives `409 TRANSACTION_NOT_OPEN` if the transaction completed, is processed normally if the transaction reopened, or receives `500 INTERNAL_ERROR` with no change if the attempt is still pending.
8. **Given** a checkout message cannot be processed after the bounded number of retries, **When** it is moved to the dead-letter destination, **Then** the message is visible to operators, the affected attempt stays pending rather than being settled on an assumed outcome, an alarm is raised, and other transactions' messages keep flowing.
9. **Given** restoration messages for the reserved stock-restoration operation are sent directly to Inventory Service, **When** they are repeated, duplicated, interrupted by an Inventory crash, redelivered after restart, or delivered before a delayed stock request for the same attempt, **Then** units are restored at most once and only if they were deducted, and the delayed stock request has no effect. This is a direct Inventory test, not an end-to-end Saga compensation test.
10. **Given** any combination of the faults above, **When** all checkouts have settled, **Then** for every SKU, initial stock minus final stock equals the units in COMPLETED transactions, and no stock is negative.

---

### User Story 4 - Keep Catalog Replicas Current Through Inventory's Events (Priority: P2)

As the store, Transaction Service validates and prices scans against product information that Inventory Service publishes as events, including the initial load after a fresh seed, so that no service ever asks another for the catalog synchronously. [C-II][C-III][Architectural Constraints]

**Why this priority**: The replica supports every Scan, but once it is populated it changes rarely. It must be populated before a benchmark and must never be filled by a synchronous call.

**Independent Test**: Reset and seed all services. Start Transaction Service before and then after Inventory Service, and verify that its replica reaches Inventory's full catalog through messages alone. Reseed Inventory with a different catalog and verify that the replica converges. Restart Transaction Service while the broker is down and verify that scans use the durable replica.

**Acceptance Scenarios**:

1. **Given** Inventory has just been seeded and Transaction's replica is empty, **When** both services and the broker are running, **Then** Transaction's replica converges to Inventory's full catalog through Inventory-published events, without any synchronous call and without waiting for a catalog change.
2. **Given** Transaction Service starts after the catalog events were first published, **When** it comes up with an empty or outdated replica, **Then** it still obtains the full current catalog through messages.
3. **Given** Inventory is reseeded with a different catalog, **When** its catalog events are consumed, **Then** the replica reflects the new catalog, and an older update delivered late never overwrites a newer one.
4. **Given** the replica was populated and then the broker and Inventory become unavailable, **When** Transaction Service restarts, **Then** scans keep using the durable replica with correct names and prices.
5. **Given** popular items are queried, **When** a window is published, **Then** each SKU carries its catalog name, obtained without Analytics reading another service's data or calling another service.

---

### User Story 5 - Recover Accurate Popular Items From Broker-Delivered Scans (Priority: P2)

As an operator, I see popular items that count every accepted scan exactly once in deterministic windows, despite duplicate, delayed, or out-of-order delivery and restarts of either service or the broker. [C-VII]

**Why this priority**: Scans now reach Analytics only through the broker, so broker delivery semantics determine analytics correctness.

**Independent Test**: Submit a controlled stream of scans with known SKUs. Inject duplicate deliveries, redelivery after lost acknowledgements, and reordering. Crash Analytics at receipt, position assignment, window computation, and persistence. Crash Transaction Service during publication, and restart the broker mid-stream. Compare every published window with an independent calculation over Analytics Service's durable positions.

**Acceptance Scenarios**:

1. **Given** fewer than 1,000 accepted scans, **When** popular items are queried, **Then** the established empty response is returned.
2. **Given** 1,000 accepted scans, **When** Analytics catches up, **Then** the first published window covers positions 1–1,000. After 500 more scans, the next covers 501–1,500.
3. **Given** a scan message is delivered more than once, **When** Analytics processes it, **Then** it is counted once and holds one position.
4. **Given** scan messages arrive out of order or late, **When** positions are assigned, **Then** every accepted scan holds exactly one immutable position, and positions are unique and contiguous from 1.
5. **Given** Analytics, Transaction Service, or the broker crashes at any processing boundary, **When** it restarts, **Then** every eligible window eventually appears once with correct counts, and no partial or duplicate window is ever visible.
6. **Given** Analytics or the broker was unavailable while scanning continued, **When** both return, **Then** Analytics processes the backlog and publishes windows in order with correct overlap.
7. **Given** tied counts across more than ten SKUs, **When** a window is published, **Then** at most ten items appear, ordered by descending count and then ascending SKU.
8. **Given** a transaction's checkout failed or was never attempted, **When** popularity is computed, **Then** its accepted scans count like any others.

---

### User Story 6 - Keep One Unchanged Client Interface (Priority: P2)

As a station or evaluator, I use the existing base URL and seven routes without changing the Load Client or knowing that a broker exists. [Contract][Client][C-IV]

**Why this priority**: Architecture iterations are comparable only when the public contract and the client stay unchanged.

**Independent Test**: Run the existing contract tests and the unchanged Load Client through the single base URL. Then stop each service and the broker in turn, and exercise every route.

**Acceptance Scenarios**:

1. **Given** all services and the broker are running, **When** the unchanged Load Client and the contract tests use one base URL, **Then** paths, methods, request and response fields, success statuses, validation errors, and domain errors match the established contract.
2. **Given** one service or the broker is stopped, **When** any route is requested, **Then** it behaves as specified in *Availability and Failure Behavior*.
3. **Given** a pending event, internal timeout, unavailable broker or dependency, or internal error affects a client request, **When** the client receives the response, **Then** it is a non-success response as defined in FR-024, reveals no broker or message details, and is never a fabricated success or a misclassified validation error.

---

### User Story 7 - Operate and Reset Each Service and the Broker Independently (Priority: P2)

As a maintainer, I build, start, stop, restart, reset, and seed each service on its own, run and reset the broker as a documented component, and each service can reach only its own data. [C-I][C-II][C-VIII]

**Why this priority**: Independent deployment and a reproducible baseline are prerequisites for trustworthy benchmark evidence, and the broker is now part of that baseline.

**Independent Test**: Install and start the broker from its documentation. Build and start each service alone. Attempt cross-service data access with each service's credentials. Reset and seed each service through its own mechanism, and reset the broker's destinations. Restart each service, and then the broker, during a workload.

**Acceptance Scenarios**:

1. **Given** only one service's artifacts and configuration plus a running broker, **When** that service is built and started, **Then** it runs without building or starting the other services.
2. **Given** each service's database credentials, **When** they are used to read or change another service's data, **Then** access is denied.
3. **Given** the documented baseline procedure, **When** all three services are reset and seeded and the broker's destinations are reset or drained, **Then** Inventory holds the 2,000-product, 10,000-units-per-product baseline, Transaction holds no business data other than its catalog replica, Analytics holds no business data, no undelivered message from an earlier run remains, and no service wrote another service's data.
4. **Given** a workload is running, **When** any single service or the broker is restarted, **Then** the remaining components keep serving as specified in *Availability and Failure Behavior*, and after the restart every invariant in Success Criteria holds.

---

### User Story 8 - Reproduce Benchmark Evidence and the Architecture Diagram (Priority: P3)

As an evaluator, I open the repository and find two genuine Load Client reports produced against the event-based deployment, compared with the prior microservices iteration, and an architecture diagram that shows the microservices, their databases, and the broker with its message flows. [Assignment][C-IX]

**Why this priority**: These are the graded deliverables, and they are meaningful only after correctness is established.

**Independent Test**: Reset all services and the broker, run the default command, and verify invariants after all work settles. Repeat with a separate reset for the stress command. Inspect both reports, the completion counts recorded with them, and the diagram, and check the diagram against the documented message catalog.

**Acceptance Scenarios**:

1. **Given** a fresh baseline, **When** `java -cp out Main` runs with its defaults of 10 stations for 60 seconds, **Then** a genuine JSON report is produced and committed.
2. **Given** a separate fresh baseline, **When** `java -cp out Main --stations=100 --duration=120` runs, **Then** a distinct genuine JSON report is produced and committed.
3. **Given** either run has finished and all work has settled, **When** results are reviewed, **Then** `totalTransactions`, throughput, per-operation error rates, and p95/p99 latency are reported and compared with [Prior reports]; durable COMPLETED transactions and background completions are recorded alongside the report; no message remains in a dead-letter destination; and the stock, transaction, and analytics invariants are verified.
4. **Given** the committed architecture diagram, **When** it is compared with the deployment and the message catalog, **Then** it shows the API Gateway, the three microservices, each service's database, the message broker, and every topic or queue with its producer and consumers, and it contains no synchronous service-to-service link.

### Edge Cases

**Messaging and the broker**

- The broker is unavailable when a service commits a state change. The change and its outgoing message commit together locally, and the message is published when the broker returns. [FR-010]
- The broker accepts a publish but its confirmation is lost. The relay publishes again, and the consumer's idempotency absorbs the duplicate. [FR-010][FR-011]
- A consumer commits its local effect but crashes before acknowledging. The message is redelivered and has no further effect. [FR-011]
- A consumer receives a message with an unknown schema version or a malformed body. It is retried a bounded number of times, then moved to the dead-letter destination, and an alarm is raised. It never blocks unrelated routing keys indefinitely. [FR-013][FR-014]
- The broker restarts and loses no acknowledged message. A message published but not yet confirmed is published again from the outbox. [FR-007][FR-010]
- Messages for the same transaction arrive out of order, for example a stale outcome for an earlier attempt after a newer attempt began. The stale message affects only its own attempt and never settles another attempt. [FR-012][FR-035]
- The broker's storage fills or the broker refuses publishes. Outbox entries stay durable in their producers, Scan continues, and Complete behaves as when the broker is unavailable. [FR-015][FR-037]
- A dead-letter destination receives a checkout message. The attempt stays pending under an integrity alarm, and it is not settled on an assumed outcome. [FR-038]

**Checkout and Saga**

- A stock deduction is applied but its outcome message is never published because Inventory crashed before its outbox relay ran. After restart, the outcome is published from Inventory's outbox. [FR-010][FR-037]
- A delayed or duplicated stock request reaches Inventory after Inventory recorded that attempt's outcome. Inventory publishes the recorded outcome again and changes no stock. If the attempt's stock was restored through the reserved operation, the request has no effect. [FR-035][FR-039]
- Inventory has no record of an attempt when Transaction Service resends it, because the earlier request message was never published or never consumed. The attempt has not failed, and Inventory processes it as new. [FR-037]
- A reserved restoration message is duplicated, redelivered after a restart, or delivered before the deduction it reverses. Units are restored at most once, and only if they were deducted. [FR-039]
- One SKU in a multi-SKU basket lacks stock. No SKU is deducted. [FR-033][FR-034]
- Concurrent attempts compete for the same SKUs, with basket lines in any order. Stock is never oversold or negative, and contention is never reported as insufficient stock. [FR-033]
- A basket contains a SKU that Inventory does not recognize, for example because the replica was out of date. Inventory rejects the attempt definitively, nothing is deducted, the client receives `500 INTERNAL_ERROR` rather than success or `INSUFFICIENT_STOCK`, and the transaction returns to OPEN. [FR-037]
- A Scan races with a Complete, or two Complete requests race, for the same transaction. At most one attempt runs, no unit joins a basket that is being completed, and each response matches a serial order. [FR-042]
- A checkout completes after its client received `500 INTERNAL_ERROR`. Lookup shows COMPLETED with its item count and total, a repeated Complete returns `409 TRANSACTION_NOT_OPEN`, and the run's evidence counts it as a background completion. [FR-037][FR-043][FR-061]
- The broker or Inventory stays unavailable for a long time. Pending attempts keep their baskets frozen and settle when both return, each under its own identifier. An attempt that Inventory never processed is evaluated against the stock at that time, so it may be rejected and reopen. Meanwhile, lookup shows OPEN, and Scan and Complete for those transactions wait up to the bound and then return `500 INTERNAL_ERROR`. [FR-037][FR-042]
- Two Transaction Service processes run, and the outcome event for a checkout is consumed by the process that is not holding the client's request. That process settles the attempt durably. The waiting request receives `500 INTERNAL_ERROR` when its wait ends, a repeated Complete returns `409 TRANSACTION_NOT_OPEN` with no further deduction, and lookup shows the settled status. [FR-044]
- Service clocks differ. Each timestamp comes from the service that owns it, message occurrence times are informational, and no correctness decision compares clocks across services.

**Scans, catalog, and transactions**

- Transaction Service has never received the catalog. Start and Lookup work, and Scan returns `500 INTERNAL_ERROR`. [FR-030]
- A catalog update and an older catalog update for the same SKU arrive out of order. The newer one stands. [FR-012][FR-017]
- Inventory is reseeded with the same benchmark catalog. Its catalog events leave the replica unchanged in content. [FR-018]
- A request names both an unknown transaction and an unknown SKU. The response is `404 TRANSACTION_NOT_FOUND`. For a completed transaction and an unknown SKU, it is `409 TRANSACTION_NOT_OPEN`. [FR-027]
- A scan response is lost after commit. The client cannot tell whether the unit was added, and a retry adds another unit, as today. Redelivery of the same scan message does not count again in Analytics.

**Analytics**

- A scan message is delivered twice, or redelivered after a lost acknowledgement. It counts once. [FR-049]
- Analytics crashes after persisting a window but before recording its progress or acknowledging the messages. The boundary is not published twice, and recomputing it gives the same result. [FR-054][FR-055]
- The broker's own record of consumption progress is lost or reset while Analytics keeps its database. Analytics resumes from its own durable progress, and redelivered messages are deduplicated. [FR-049]
- `limit` is 0, greater than 10, or invalid. As today, the response contains no items, at most ten items, or `400 INVALID_LIMIT`, respectively. [FR-054]

**Isolation, operations, and benchmarks**

- A service's credentials are used against another service's data. Access is denied. [FR-004]
- The broker is reset while a service still holds unpublished outbox entries from an earlier run. The baseline procedure resets services and the broker together, so no stale message crosses into a new run. [FR-059]
- Requests time out at the client under stress. Each timed-out request's effect is all or nothing, and invariants are checked against durable state rather than client counts.

## Requirements *(mandatory)*

### Functional Requirements

**Service boundaries**

- **FR-001 [C-I]**: The system MUST consist of three independently deployable services, Transaction, Inventory, and Analytics, each with its own runtime, entry point, configuration, ports, and database. Apart from a client-facing gateway and the message broker, no other component may be added without a documented business need.
- **FR-002 [C-I]**: Transaction Service MUST own the transaction lifecycle, baskets, accepted scans and their captured prices, checkout Saga state and its durable progress, its catalog replica, and its outgoing messages. Inventory Service MUST own the catalog, prices, stock, low-stock thresholds, the durable record of inventory operations, and its outgoing messages. Analytics Service MUST own received scan messages, analytics positions, windows, rankings, and consumption progress.
- **FR-003 [C-I]**: Transaction Service MUST serve `/transactions/**`, Inventory Service MUST serve `/items` and `/inventory/low-stock`, and Analytics Service MUST serve `/analytics/popular-items`. Business rules MUST stay in their owning service and MUST NOT live in message listeners. Shared code, such as message schemas, MUST NOT contain cross-domain business logic or create runtime coupling between services.

**Database per service**

- **FR-004 [C-II]**: Each service MUST keep its data in its own database or schema, accessed through credentials limited to it. No query, join, reference, or transaction may span two services' data, and no service may read or change another service's data.
- **FR-005 [C-II]**: Acknowledged state MUST be durable in its owning service's database. The broker MUST NOT be the sole record of any service's business state, and in-process caches, wait registries, or retry schedules MUST NOT be the sole authority for durability, idempotency, or inventory correctness.

**Event-based communication**

- **FR-006 [C-III]**: All communication between services MUST be asynchronous messages carried by the message broker. No synchronous service-to-service call, whether a remote procedure call, a REST call, or a direct socket, may remain. The feature 005 synchronous internal interfaces, their listeners, and their dependencies MUST be removed once their replacements are verified. External requests that reach a service through the gateway are not service-to-service communication. Every message MUST be a domain event, named and worded as a fact that its producer has already recorded, never as a command addressed to another service.
- **FR-007 [C-VIII]**: The system MUST use exactly one message broker product. The choice, its delivery and ordering guarantees, persistence settings, and rejected alternatives MUST be recorded in the feature's research. The broker MUST store messages durably, so that a publish it has confirmed survives a broker restart.
- **FR-008 [C-III]**: Every message type MUST have a version-controlled schema. Every message MUST carry a globally unique message identifier, its type, its schema version, a routing key, its occurrence time, and a correlation identifier that links it to the originating transaction, checkout attempt, or request. Messages MUST NOT expose storage details.
- **FR-009 [C-III]**: Each message type MUST have exactly one owning producer service. Every destination (topic, queue, or exchange), its routing keys, its consumers and consumer groups, and its dead-letter destination MUST be documented in a message catalog that covers at least the flows in *Required Message Flows*.
- **FR-010 [C-VII]**: Every message MUST first be recorded in its producer's database in the same local transaction as the state change that causes it. A relay MUST publish recorded messages with retries and MUST mark them published only after the broker confirms receipt. Publishing a message that affects business state or analytics without such a durable record is prohibited.
- **FR-011 [C-III]**: Delivery MAY be at least once. Every consumer MUST be idempotent, keyed by the message identifier or a stable business key, and MUST acknowledge a message only after its local database effects have committed. Duplicate or redelivered messages MUST NOT produce a second business effect.
- **FR-012 [C-III]**: Messages MUST be processed in order per routing key wherever business rules depend on order: per transaction for checkout messages, per SKU or per catalog version for catalog messages, and in the documented order for analytics. A late or stale message MUST NOT overwrite newer state. The plan MUST state each ordering guarantee and how the broker provides it.
- **FR-013 [C-III]**: A message that cannot be processed after a bounded number of retries MUST be moved to a dead-letter destination or an equivalent parked state. Parked messages MUST be observable and MUST NOT be silently dropped, and they MUST NOT block unrelated routing keys indefinitely.
- **FR-014 [C-III][C-IX]**: Schema changes MUST be backward compatible for consumers already deployed, or MUST be introduced as a new versioned message type. A producer and a consumer built separately from the same schema version MUST interoperate, as shown by contract tests against real producers and consumers.
- **FR-015 [C-III][C-VII]**: Start, Scan, and Lookup MUST NOT wait for the broker or for another service. A broker outage MUST NOT block them.
- **FR-016 [C-IV]**: Broker details MUST remain invisible to clients. Broker unavailability, pending messages, and message timeouts MUST be translated into the established external error contract (FR-024).

**Replicated data**

- **FR-017 [C-II][Architectural Constraints]**: Transaction Service MUST keep a durable local replica of the product information needed to validate scans and capture names and prices, with Inventory Service as the source of truth. The replica MUST be populated and kept current only through Inventory-published messages, including its initial load. Once populated, it MUST keep Scan available during broker and Inventory outages, including across a Transaction Service restart. It MUST NOT be used for any stock decision.
- **FR-018 [C-II]**: After every Inventory reset or reseed, and whenever a consumer starts with an empty or outdated replica, the consumer MUST be able to obtain Inventory's full current catalog through messages, without a synchronous call and without waiting for a later catalog change. The plan chooses the mechanism, and any request message it introduces has its own single producer (FR-009).
- **FR-019 [C-II][C-VII]**: Popular items MUST show each SKU's catalog name without Analytics Service reading another service's data or calling another service. The plan chooses whether names travel with scan messages or come from an Analytics replica fed by Inventory's catalog messages.
- **FR-020 [C-II]**: Each replica MUST have a documented source of truth, message-based synchronization path, ordering rule, staleness bound, and behavior while its source or the broker is unavailable. Before a benchmark run begins, replicas MUST reflect Inventory's catalog as it stands after the latest Inventory reset or reseed.

**External compatibility**

- **FR-021 [Contract][C-IV]**: All seven existing routes MUST keep their paths, methods, request and response fields, success statuses, error statuses and codes, validation precedence, and business semantics, as summarized in *Established External Behavior*. `spec/self-checkout-openapi.yaml` remains authoritative and unchanged.
- **FR-022 [C-IV]**: One client-facing base URL, by default `http://localhost:8080`, MUST route each request to its owning service. The routing MUST be documented in deployment configuration.
- **FR-023 [Client][C-IV]**: The Load Client MUST run without any change to its source, defaults, or behavior.
- **FR-024 [C-IV]**: When a reachable service cannot give a definitive result because an outcome is pending, the broker or a dependency is unavailable or slow, or an internal error occurred, it MUST return the existing `500 INTERNAL_ERROR` response with the established `{error, message}` body. Established 4xx business errors, including every 409, keep their current meaning and precedence. When an owning service is unreachable, the gateway's existing 502 response applies. A 500 from an unsettled Complete means only that the outcome is not yet known and MUST NOT be treated as a cancellation. None of these conditions may produce a success response or a 4xx business error.

**Transactions and scans**

- **FR-025 [Contract][C-II]**: Start MUST durably create an OPEN transaction before returning HTTP 201. Lookup MUST return the current durable status, item count, running total, and start time, including after restarts.
- **FR-026 [Contract][C-VI][C-VII]**: Each successful Scan MUST add exactly one unit at the unit price recorded on that SKU's basket line, captured from the replica when the SKU is first accepted into the transaction. The basket change and its accepted-scan message MUST commit in one local transaction before HTTP 200. A rejected Scan MUST commit neither. A repeated identical Scan request adds another unit, as today.
- **FR-027 [Contract]**: Scan validation MUST keep its established precedence: request validation (400), then unknown transaction (`404 TRANSACTION_NOT_FOUND`), then a transaction that is not open (`409 TRANSACTION_NOT_OPEN`), then an unknown SKU (`404 SKU_NOT_FOUND`).
- **FR-028 [C-VI]**: Scanning MUST NOT change stock.
- **FR-029 [C-II]**: Acknowledged transactions, basket lines, captured prices, and accepted-scan messages MUST survive a Transaction Service crash and restart without manual reconstruction.
- **FR-030 [C-II]**: If Transaction Service has no usable product information, for example on first start before any catalog message has arrived, Scan MUST return `500 INTERNAL_ERROR` (FR-024) rather than `404 SKU_NOT_FOUND`.

**Distributed checkout**

- **FR-031 [C-V]**: Complete MUST run as a Saga whose steps are carried by broker messages, without any transaction or two-phase commit that spans service databases or a database and the broker. The Saga MUST use choreography with domain events, not commands and replies: Transaction Service publishes a checkout-requested event for each attempt (called the stock request below), and Inventory Service reacts by publishing a stock-deducted, stock-rejected, or equivalent outcome event (called the outcome below). Transaction Service MUST still durably own the checkout's Saga state, and the plan MUST define every step, its message types, its local transaction, its compensating action or retry and idempotency rule, and the persisted states, consistent with *Checkout Recovery Model*.
- **FR-032 [Contract][C-V]**: Complete MUST check, in this order, for an unknown transaction (`404 TRANSACTION_NOT_FOUND`), a transaction that is not open (`409 TRANSACTION_NOT_OPEN`), and an empty basket (`409 EMPTY_BASKET`) before publishing any checkout message.
- **FR-033 [C-V][C-VI]**: Inventory Service MUST apply a checkout attempt's stock effect for all basket lines atomically: all lines or none. Concurrent attempts MUST NOT oversell, lose updates, or drive stock negative. Contention or transient faults inside Inventory MUST NOT be reported as insufficient stock.
- **FR-034 [Contract][C-V]**: If any line lacks stock, Inventory MUST reject the attempt definitively and change no stock, and the transaction MUST return to OPEN with its basket. When the rejection reaches Transaction Service before the response deadline, Complete MUST return `409 INSUFFICIENT_STOCK` naming such a SKU.
- **FR-035 [C-V]**: Every checkout message MUST carry a stable checkout-attempt identifier associated with the transaction ID. Inventory Service MUST durably record each attempt's outcome, in the same local transaction as any stock change and together with its outgoing outcome message. It MUST answer any repeated or redelivered request for the same attempt with the original outcome. Each attempt's units MUST be deducted at most once and restored at most once.
- **FR-036 [C-V][C-VI]**: A transaction may make successive checkout attempts, but a new attempt is allowed only after the previous one settled without a standing deduction, either because it was definitively rejected or because its stock was restored through the reserved operation (FR-039). A transaction MUST have at most one pending attempt at a time and MUST NOT have more than one effective inventory deduction.
- **FR-037 [C-V][C-VI]**: If an attempt does not settle before its response deadline, or a crash interrupts it, the client MUST NOT receive success, and a reachable Transaction Service MUST return `500 INTERNAL_ERROR`. The attempt MUST remain durably pending. A timeout, lost or delayed message, broker outage, or crash MUST NOT by itself be treated as an inventory failure or trigger stock restoration. Transaction Service MUST automatically publish the request again under the same attempt identifier, on a documented schedule, until it receives a definitive outcome message, and then settle the attempt. It settles as COMPLETED if Inventory applied the deduction. It settles as OPEN with the basket intact if Inventory definitively rejected the attempt for insufficient stock or an invalid request, or reports that its stock was restored. Lookup MUST show the settled state through the existing contract.
- **FR-038 [C-V][C-VI]**: Transaction Service MUST NOT finalize or reopen an attempt on an assumed outcome. A checkout message that is dead-lettered, malformed, or inconsistent with the attempt it names is an integrity fault: the attempt MUST stay pending, and an alarm visible to operators MUST be raised.
- **FR-039 [C-V]**: Normal checkout MUST use forward recovery. Once Inventory applies an attempt's deduction, the attempt MUST end COMPLETED. A definitive rejection MUST reopen the basket, which is the compensating action for the attempt's first step. Inventory Service MUST also keep a durable, idempotent stock-restoration operation as a reserved capability that no normal checkout path invokes:
  - It MUST be invoked through a restoration message with a single documented producer, and its result MUST be published as a message.
  - It MUST restore exactly the units the attempt deducted, at most once, across duplicates, crashes, and redelivery.
  - It MUST prevent any later stock request for the same attempt from taking effect.
  - It MUST NOT be used for an attempt whose transaction is COMPLETED. If it is used for a pending attempt, Transaction Service MUST treat that attempt as definitively not applied and reopen the basket.
  - The plan MUST document any operational use.
  - It MUST be verified by direct tests against Inventory Service, and those tests MUST NOT be reported as end-to-end Saga compensation tests.
- **FR-040 [C-V][C-VIII]**: Saga progress MUST be persisted in Transaction Service's database together with the outgoing message for each step. No database connection, lock, or open database transaction may be held while waiting for an outcome event. Recovery MUST begin automatically after a restart of any service or of the broker.
- **FR-041 [C-V][C-VI]**: Complete MUST return HTTP 200 with the established receipt only after Inventory has durably committed the attempt's deduction and Transaction Service has durably committed COMPLETED. Receipt lines and totals MUST use the captured prices.
- **FR-042 [Contract][C-V]**: While a checkout attempt is pending, a Scan or Complete for the same transaction MUST wait for the attempt to settle, for a bounded time that ends within the Load Client's default 10-second request timeout. It MUST then be applied in serial order against the settled state: if the transaction is COMPLETED, the request receives `409 TRANSACTION_NOT_OPEN`; if the transaction is OPEN again, the request is processed normally. If the attempt has not settled when the wait ends, the request MUST receive `500 INTERNAL_ERROR` and MUST NOT change the basket, start another attempt, or cancel the pending attempt. Waiting MUST NOT hold a database connection, lock, or open database transaction. Lookup MUST answer without waiting and MUST report OPEN until COMPLETED is durable.
- **FR-043 [Contract][C-V]**: Repeating Complete after a transaction is COMPLETED MUST return `409 TRANSACTION_NOT_OPEN` with no further stock effect, including after a 500 response, restarts of any service or the broker, and duplicated or delayed messages.
- **FR-044 [C-V][Client]**: Waiting for outcome events, and waiting under FR-042, MUST be bounded so that, at the default benchmark load or lighter, every Scan and Complete response arrives within the Load Client's default 10-second request timeout, even when the broker or Inventory is unavailable or unresponsive. With a single Transaction Service process, a settled outcome MUST reach the waiting request before its deadline. With more than one process, outcome events need not be routed to the process that holds the waiting request, and no reply routing or other infrastructure may be added only for that purpose. A request that misses its answer receives `500 INTERNAL_ERROR` (FR-024), the attempt settles in the background under FR-037, a repeated Complete causes no further deduction (FR-043), and lookup shows the settled status. Every durable Saga, stock-consistency, idempotency, and recovery guarantee MUST hold for any number of processes.
- **FR-045 [C-VI]**: Each transaction MUST end with exactly one client-observable outcome that agrees with Inventory's recorded effects. After all attempts settle, for every SKU, initial stock minus final stock MUST equal the units in COMPLETED transactions, and no stock may be negative, including after redelivery, crashes, broker restarts, and any use of the reserved restoration operation.
- **FR-046 [C-VIII][C-IX]**: Each service MUST provide documented operational means, outside the public REST contract, to tell whether recovery work is pending. These cover pending checkout attempts, unpublished outbox entries, unconsumed or parked messages, integrity alarms, and eligible windows not yet published. Transaction Service MUST also be able to report the background completions that FR-061 counts.

**Reliable analytics**

- **FR-047 [C-VII]**: Each accepted-scan message MUST have a stable, unique identity and MUST be recorded in the same local transaction as its basket change (FR-026).
- **FR-048 [C-VII]**: Accepted scans MUST reach Analytics Service only through the broker, asynchronously and at least once. Analytics Service MUST NOT read Transaction Service's data. Scan and Complete MUST NOT wait for Analytics or for publication. Undelivered messages MUST remain durable in Transaction Service until the broker confirms receipt.
- **FR-049 [C-VII]**: Analytics Service MUST deduplicate by message identity and durably record its consumption progress in its own database, so that duplicates, redelivery, and restarts of Transaction Service, Analytics Service, or the broker neither lose nor double-count scans. The broker's record of consumption progress MAY be used as an optimization but MUST NOT be the sole record.
- **FR-050 [C-VII]**: Each distinct accepted scan MUST receive exactly one analytics position, stored durably by Analytics Service. Positions MUST be unique, contiguous from 1, and immutable. The ordering rule MUST be documented. Late or out-of-order delivery MUST NOT create a gap, a duplicate, or a reassignment.
- **FR-051 [C-VII]**: The first complete window MUST cover positions 1–1,000, and later windows MUST end every 500 positions, each containing exactly the latest 1,000 positions. Boundaries MUST NOT shift after restart or replay.
- **FR-052 [C-VII]**: Each window MUST rank SKUs by descending accepted-scan count, break ties by ascending SKU, number ranks contiguously from 1, and keep at most ten. Popularity counts accepted scans, not completed sales.
- **FR-053 [C-VII]**: Window computation, ranking, and persistence MUST remain separate stages with ordered, asynchronous hand-off and immutable data between them.
- **FR-054 [Contract][C-VII]**: A window and all its ranks MUST become visible together, only after durable persistence, and in window order. Popular items MUST return the latest such window, or the established empty response, with the established `limit` behavior.
- **FR-055 [C-VII]**: After an Analytics crash at any processing boundary, including before acknowledging consumed messages, every accepted scan MUST appear exactly once in every eligible window, with no duplicate publication, using only Analytics Service's own durable data. Analytics may lag behind accepted scans; the plan MUST define the visibility delay under healthy operation, and lag MUST appear only as an older complete window.

**Operations**

- **FR-056 [C-VIII]**: Each service and the broker MUST document build or installation, startup, shutdown, restart, ports, configuration, database or storage connection, credential handling, destinations, and dependencies. Each service MUST be buildable, startable, stoppable, and restartable on its own while the broker runs.
- **FR-057 [C-VIII]**: Restarting any single service or the broker MUST preserve overall correctness. The restarted component MUST resume from its own durable state without a peer restart.
- **FR-058 [C-II][C-VIII]**: Each service MUST provide its own reset and seed mechanism, and none may write another service's data.
- **FR-059 [C-VIII][Architectural Constraints]**: The benchmark baseline MUST consist of:
  - Inventory seeded with the current deterministic 2,000-product catalog at 10,000 units per product and a low-stock threshold of 50;
  - Transaction and Analytics holding no business data, apart from catalog replicas refreshed from Inventory's messages;
  - the broker's destinations reset or drained, and no unpublished outbox entry left from an earlier run.

  One documented procedure MUST produce this baseline.
- **FR-060 [C-VIII]**: Unavailability of a service, of its database, or of the broker MUST produce the behavior in *Availability and Failure Behavior*.

**Performance, evidence, and verification**

- **FR-061 [C-VIII][C-IX]**: The primary benchmark metric MUST remain the unchanged Load Client's `totalTransactions`. Each run MUST also report throughput, per-operation success and error rates, and p95/p99 latency, compared with [Prior reports]. Alongside each report, and without modifying the Load Client's JSON, the evidence MUST record the following from durable state:
  - the number of COMPLETED transactions;
  - the number of background completions;
  - any remaining difference from `totalTransactions`;
  - the number of dead-lettered messages, which is expected to be zero.

  No numerical improvement target applies, and correctness MUST NOT be traded for throughput.
- **FR-062 [Assignment][C-IX]**: Two genuine JSON reports MUST be produced by the unchanged Load Client against the actual event-based deployment and committed: one from `java -cp out Main` (10 stations, 60 seconds) and one from `java -cp out Main --stations=100 --duration=120`, each after a separate fresh baseline (FR-059). Each report MUST identify its settings and the event-based deployment used, and invariant checks MUST follow each run after all work settles. Fabricated reports and reports from earlier architectures are not evidence.
- **FR-063 [Assignment][C-IX]**: An architecture diagram MUST be committed in a form viewable directly in the repository's web view. It MUST show the API Gateway, the three microservices, each service's database, the message broker, and every topic or queue with its producer and consumers, and it MUST agree with the message catalog (FR-009). It MUST NOT show any synchronous service-to-service link.
- **FR-064 [C-IX]**: Automated tests MUST cover:
  - concurrent checkouts with stock reconciliation;
  - Saga success and insufficient stock with the basket reopened, the end-to-end compensation path;
  - forward recovery after timeouts, lost, duplicated, delayed, and redelivered messages, and crashes of Transaction Service, Inventory Service, and the broker at each Saga step;
  - bounded waiting during a pending attempt;
  - dead-letter handling and integrity alarms;
  - the reserved stock-restoration operation, tested directly against Inventory Service and reported separately;
  - catalog replication, including the initial load, a reseed, and out-of-order updates;
  - analytics windows, rankings, ties, positions, duplicates, reordering, and restarts of either service or the broker;
  - message schema compatibility and producer–consumer contracts;
  - that every published message passes through an outbox;
  - that no synchronous service-to-service call remains;
  - database isolation, independent failure of each service and of the broker, external API compatibility, and the unchanged Load Client.
- **FR-065 [C-IX]**: Existing tests MUST be kept or adapted without weakening their assertions. Tests of the removed synchronous internal interfaces MUST be replaced by message-contract tests with equivalent business assertions before they are deleted. The existing architecture test that forbids message-broker dependencies conflicts with this feature and MUST be revised to forbid synchronous service-to-service dependencies instead. Each replaced assertion MUST be recorded with its governing requirement.
- **FR-066 [C-VIII]**: The plan MUST choose response deadlines, publication latency, and capacity that minimize background completions under healthy workloads. The default benchmark MUST produce none, and the stress benchmark's count MUST be reported and explained.

### Established External Behavior

Preserved from [Contract] and [Preserved]; contract tests remain the oracle.

| Route | Owner | Success | Established errors, in precedence order |
|---|---|---|---|
| `GET /items` | Inventory | 200, the full catalog in ascending SKU order (2,000 items at baseline) | — |
| `POST /transactions` | Transaction | 201, an OPEN transaction with zero items | 400 `MISSING_STATION_ID`, 400 `INVALID_REQUEST` |
| `POST /transactions/{id}/items` | Transaction | 200, the scan result | 400 `MISSING_SKU` or `INVALID_REQUEST`; 404 `TRANSACTION_NOT_FOUND`; 409 `TRANSACTION_NOT_OPEN`; 404 `SKU_NOT_FOUND` |
| `POST /transactions/{id}/complete` | Transaction | 200, the receipt | 404 `TRANSACTION_NOT_FOUND`; 409 `TRANSACTION_NOT_OPEN`; 409 `EMPTY_BASKET`; 409 `INSUFFICIENT_STOCK` |
| `GET /transactions/{id}` | Transaction | 200, the transaction | 404 `TRANSACTION_NOT_FOUND` |
| `GET /inventory/low-stock` | Inventory | 200; default threshold 50; alerts where stock is at or below the threshold, in ascending SKU order | 400 `INVALID_THRESHOLD` |
| `GET /analytics/popular-items` | Analytics | 200; default `limit` 10 and at most ten ranks; the empty response before the first complete window | 400 `INVALID_LIMIT` |

A reachable service returns 500 `INTERNAL_ERROR` for an unexpected failure, a pending outcome, or an unavailable broker or dependency (FR-024). An unreachable owning service yields the gateway's existing 502.

### Data Ownership and Replicated Information

| Information | Source of truth | Copies elsewhere | Consistency expectation |
|---|---|---|---|
| Catalog: SKU, name, price | Inventory | Transaction's product replica; Analytics' names, carried by scan messages or held in a replica | Eventually consistent through Inventory's catalog messages; fixed during benchmark runs; converges after an Inventory reseed; never used for stock decisions |
| Stock, thresholds, and inventory operation records | Inventory | None | Atomic and immediately consistent within Inventory |
| Transactions, basket lines with captured prices, checkout attempts and Saga progress | Transaction | Inventory keeps only the per-attempt quantities it needs to apply and restore stock effects | Durable before acknowledgement |
| Outgoing messages | Their producer's database (outbox) | The broker, until consumed | Durable in the producer until the broker confirms receipt |
| Accepted scans as received by Analytics, positions, windows, ranks, and consumption progress | Analytics | None | Eventually consistent with accepted scans; only complete, persisted windows are visible |

### Required Message Flows

Every flow is a domain event stream (FR-006, FR-031). The plan names the event types, destinations, and routing details. Outcome rows may be split into one event type per outcome, for example stock deducted and stock rejected.

| Flow | Producer | Consumer | Ordering key | Timing | Required contents or outcomes | Idempotency key |
|---|---|---|---|---|---|---|
| Catalog published | Inventory | Transaction; Analytics if it keeps a name replica | SKU or catalog version | After each reset or seed, on any catalog change, and whenever a consumer needs the full catalog (FR-018) | SKU, name, price, and enough version information to reject stale updates | SKU with version |
| Checkout requested (the stock request) | Transaction | Inventory | Transaction ID | During Complete, and republished while the attempt is pending | Attempt identifier and line quantities | Checkout-attempt identifier |
| Checkout stock outcome (stock deducted, stock rejected, or deduction refused) | Inventory | Transaction | Transaction ID | After Inventory commits its decision, and again for every repeated request | Applied; insufficient stock, naming a SKU, with nothing applied; invalid, for example an unknown SKU, with nothing applied; refused because the attempt's stock was already restored | Checkout-attempt identifier |
| Stock restoration requested (reserved) | One producer designated by the plan | Inventory | Transaction ID | Never by normal checkout; in direct tests and documented operational procedures; retried until confirmed | Attempt identifier | Checkout-attempt identifier |
| Stock restoration result (reserved) | Inventory | The designated producer, and Transaction if the attempt is pending | Transaction ID | After Inventory commits the restoration | Units restored; nothing to restore, with the attempt now blocked; already restored | Checkout-attempt identifier |
| Scan accepted | Transaction | Analytics | Defined by the plan's analytics ordering rule | Asynchronous to Scan, at least once | Message identity, transaction, SKU, captured price, acceptance time, and the SKU name if names travel with scans | Message identity |

### Checkout Recovery Model

Normal checkout uses forward recovery. The plan defines the local transactions, persisted states, and mechanisms behind each step.

| Step | Service | Effect | Definitive failure | Uncertain outcome |
|---|---|---|---|---|
| 1. Begin attempt | Transaction | Durably records a pending attempt with its identifier, freezes the basket, and records the stock-request message in the same local transaction | If the local commit fails, the transaction stays OPEN with no attempt and no message | Recovery finds any committed attempt and republishes its request |
| 2. Apply stock effect (pivot) | Inventory | On consuming the request, deducts all lines atomically under the attempt identifier, or rejects the attempt with nothing applied, and records the outcome message in the same local transaction | A rejection reopens the basket, which compensates step 1. A waiting client receives `409 INSUFFICIENT_STOCK`, or `500 INTERNAL_ERROR` for an invalid request. | Forward recovery: republish under the same identifier until an outcome arrives; never restore stock |
| 3. Record completion (retriable) | Transaction | On consuming an Applied outcome, marks the transaction COMPLETED and answers any waiting client with the receipt | Cannot fail definitively; redelivery retries it | Redelivered outcomes are idempotent |
| Reserved: restore stock | Inventory | On consuming a restoration message, restores exactly the attempt's units, at most once, blocks later stock requests for that attempt, and publishes the result | — | Idempotent; retried until confirmed |

### Transient and Settled Guarantees

An attempt is **pending** from the moment Transaction Service records it until Transaction Service records its settlement. It is **settled** once it is COMPLETED or once it is rejected and the basket is OPEN again. The guarantees in feature 005's table of the same name carry over unchanged. In addition:

| Guarantee | When it holds | Requirements |
|---|---|---|
| A confirmed publish survives a broker restart, and an unconfirmed one is still recorded in its producer's outbox | At all times | FR-007, FR-010 |
| A message's business effect is applied at most once, however often it is delivered | At all times | FR-011, FR-035, FR-049 |
| A message can be on the broker, or unacknowledged, after its effect committed | Transient | FR-011 |
| A pending attempt can have its deduction applied in Inventory while its outcome message is unpublished or unconsumed, and lookup shows OPEN | Transient, while pending | FR-037, FR-042 |
| No dead-lettered checkout message settles an attempt | At all times | FR-038 |
| Initial stock minus final stock equals the units in COMPLETED transactions | Once all attempts settle | FR-045, SC-002 |

### Availability and Failure Behavior

| Condition | Required observable behavior |
|---|---|
| Broker unavailable | Start, Scan, and Lookup continue for transactions without a pending checkout. `/items`, `/inventory/low-stock`, and `/analytics/popular-items` keep answering from their owners' data, with Analytics serving its latest window. Complete returns `404 TRANSACTION_NOT_FOUND`, `409 TRANSACTION_NOT_OPEN`, or `409 EMPTY_BASKET` where they apply; otherwise it records a pending attempt and returns `500 INTERNAL_ERROR` within 10 seconds. Outgoing messages stay in their producers' outboxes. When the broker returns, all messages are published, pending attempts settle, and Analytics catches up. |
| Broker restarts | No confirmed message is lost. Unconfirmed messages are published again from outboxes. Consumers resume and absorb duplicates. |
| Inventory Service unavailable | `/items` and `/inventory/low-stock` return the gateway's 502. Start, Scan, and Lookup continue. Complete behaves as when the broker is unavailable, and its stock requests wait on the broker until Inventory consumes them. A newly started Load Client cannot begin, because it fetches `/items` first. |
| Analytics Service unavailable | `/analytics/popular-items` returns the gateway's 502. All other routes are unaffected. Scan messages wait on the broker and are consumed after restart. |
| Transaction Service unavailable | `/transactions/**` return the gateway's 502. Inventory and Analytics routes continue. Messages addressed to Transaction wait on the broker. On restart, pending checkouts resume and settle, and unpublished messages are published. |
| A service's own database unavailable | That service's database-dependent operations return `500 INTERNAL_ERROR` without acknowledging uncommitted writes, and its consumers stop acknowledging messages until the database returns. Other services keep working. When databases share one server for local development, a server outage affects all services at once. |
| Any single service or the broker restarts | The remaining components keep running. The restarted component resumes from its own durable state. |

### Key Entities

- **Transaction**, **Basket Line**, **Checkout Attempt**, **Product and Stock**, **Inventory Operation Record**, **Popular Window**: as in feature 005, except that a Checkout Attempt advances only through messages, and an Inventory Operation Record is written together with its outcome message.
- **Message**: An immutable record exchanged through the broker, with a unique identifier, type, schema version, routing key, occurrence time, correlation identifier, and a body defined by its version-controlled schema. Each type has exactly one producer.
- **Outbox Entry**: A producer's durable copy of a message that it has not yet confirmed as published, written in the same local transaction as the change that caused it.
- **Processed-Message Record**: A consumer's durable record of a message identifier or business key whose effect has committed, used for deduplication.
- **Dead-Letter Entry**: A message that exhausted its retries, with the reason, kept visible until an operator handles it.
- **Catalog Replica**: Transaction Service's durable copy of each SKU's name and price, with the version information needed to reject stale updates. Analytics may keep a similar name replica. Neither is authoritative.
- **Received Scan**: Analytics Service's deduplicated copy of an accepted-scan message and its immutable position once assigned.
- **Recovery State**: Each service's durable progress: pending attempts, unpublished outbox entries, consumption progress, integrity alarms, background completions, and analytics publication progress.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001 [C-IV]**: 100% of applicable contract tests for the seven routes pass through the single base URL, and the Load Client source and the OpenAPI document are byte-identical to those of the prior iteration.
- **SC-002 [C-VI]**: In concurrent completion tests and after both benchmark runs settle, every SKU has nonnegative stock, and initial stock minus final stock equals the units in COMPLETED transactions. Repeated completions cause zero additional stock changes.
- **SC-003 [C-V]**: Across fault-injection tests that cover every checkout step; crashes of Transaction Service, Inventory Service, and the broker; and lost, duplicated, delayed, and redelivered messages, 100% of checkouts settle without manual intervention. Each is COMPLETED exactly when Inventory applied the deduction, and OPEN otherwise. Zero clients receive success for a checkout that is not COMPLETED, zero timeouts or lost messages trigger stock restoration, and zero attempts are deducted or restored more than once.
- **SC-004 [C-III][C-VIII]**: With the broker stopped, 100% of valid Start and Lookup requests, Scan requests for transactions without a pending checkout, and `/items`, `/inventory/low-stock`, and `/analytics/popular-items` requests succeed, including after a Transaction Service restart. 100% of Complete requests that pass the pre-checks return `500 INTERNAL_ERROR` within 10 seconds. After the broker returns, 100% of those attempts settle without manual intervention, and every scan accepted during the outage is counted exactly once.
- **SC-005 [C-VII][C-VIII]**: Restarting the broker during a workload causes zero lost acknowledged scans, zero lost or duplicated stock effects, and zero violations of SC-002 or SC-007.
- **SC-006 [C-II][C-III]**: With Inventory Service stopped, 100% of valid Start, Lookup, and Scan requests for transactions without a pending checkout succeed, and after Inventory returns, 100% of the attempts that were pending settle.
- **SC-007 [C-VII]**: Across controlled streams with duplicates, redelivery, reordering, and crashes at every analytics boundary, 100% of accepted scans hold exactly one position, positions run from 1 to N without gaps, 100% of eligible windows match an independent calculation, and zero partial or duplicate windows are ever visible. With Analytics stopped during a workload, zero Scan or Complete failures are attributable to it.
- **SC-008 [C-VII]**: With all components healthy, the published window reaches the highest eligible boundary within 60 seconds after the last accepted scan of either benchmark workload.
- **SC-009 [C-II][Architectural Constraints]**: After a fresh baseline, Transaction Service's replica matches Inventory's 2,000-product catalog before the first benchmark scan, and it was populated only through messages.
- **SC-010 [C-III][C-VII]**: Automated checks and review find zero synchronous service-to-service call paths, zero published messages that bypass an outbox, and zero message types with more than one producer. Every message type has a schema under version control and a catalog entry.
- **SC-011 [C-II]**: 100% of attempts to read or change another service's data with a service's credentials are denied.
- **SC-012 [C-I][C-VIII]**: Each service builds, starts, stops, and restarts on its own while the broker runs, and the broker installs, starts, and restarts from its documentation. Restarting any one of them during a workload causes zero loss of acknowledged scans and zero violations of SC-002 or SC-007.
- **SC-013 [C-IX]**: The default benchmark run, with all components healthy, records zero errors for each of the three operations, zero background completions, and zero dead-lettered messages.
- **SC-014 [Assignment][C-IX]**: Two distinct, genuine, committed JSON reports, one from the default run and one from the 100-station, 120-second run, each made after a fresh baseline, report `totalTransactions`, throughput, per-operation error rates, and p95/p99 latency. Each includes a documented comparison with [Prior reports], is accompanied by the durable counts required by FR-061, and passes the invariant checks.
- **SC-015 [Assignment][C-IX]**: The committed architecture diagram shows 100% of the components and message flows in the message catalog, and nothing that contradicts it, and opens directly in the repository's web view.
- **SC-016 [C-V][C-IX]**: In direct tests against Inventory Service, 100% of duplicated, repeated, crash-interrupted, and out-of-order restoration messages restore units at most once and only for deducted attempts, and every later stock request for a restored attempt has no effect. These results are reported separately from end-to-end Saga results.

## Assumptions

- The broker product is chosen during research, as Principle VIII requires. Any of the products the assignment names is acceptable if it provides durable storage, publish confirmation, per-key ordering, and dead-lettering or an equivalent.
- The broker runs as a single node on the same machine as the services for local development and benchmarks. It may run in a container. Clustering, replication across brokers, transport security, and authentication between services and the broker are out of scope.
- The OpenAPI document and tested handler behavior govern endpoint details. Payment always succeeds. There is no cancellation, restock, price-change, or catalog-change endpoint, and no public operation produces the CANCELLED status.
- The catalog and prices stay fixed during a benchmark run. They change only through Inventory's reset and seed or through test fixtures.
- The Load Client's default per-request timeout is 10 seconds. It never retries and counts only HTTP 200 completions in `totalTransactions`. Because an attempt answered with `500 INTERNAL_ERROR` may still complete later, reconciliation uses durable state.
- The external contract has no pending status. While a checkout is pending, lookup shows OPEN. A checkout that completes after a 500 exposes its status, item count, and total through lookup, but not its receipt lines.
- Low-stock alerts report Inventory's authoritative stock. Units deducted for a pending attempt count as unavailable, as in feature 005.
- Benchmarks run one process per service. Safety and recovery guarantees are stated for the service, not for one process, so they must also hold with more than one process. Timely Complete answers are guaranteed only with a single Transaction Service process (FR-044).
- Performance may decrease relative to the prior iteration, because Complete now waits for two broker hops instead of one synchronous call. Correctness and durability are pass conditions, and throughput is measured, not targeted.
- The 60-second analytics visibility ceiling in SC-008 matches feature 005. The plan may set a tighter bound.
- The new reports and the diagram are committed under `load-client/reports/` and the feature's documentation, following the layout used by feature 005.

## Constitution Alignment

This specification implements constitution v6.0.0 and weakens none of its MUST requirements.

- It supersedes feature 005's synchronous internal interface requirements and that feature's decision against adding a broker. Feature 005's documents stay unedited as history [Preserved].
- The architecture test that forbids broker dependencies follows constitution v5.1.0. FR-065 revises it to enforce Principle III as amended.
- The v6.0.0 amendment is a modification in the working tree that has not been committed. Under Governance, its Sync Impact Report must be removed before it is committed, and the amendment should be committed before or together with this specification.

## Planning Obligations

The plan MUST address at least the following and trace each item to the listed requirements:

1. **Broker selection**: product, version, durability and confirmation settings, ordering and dead-letter capabilities, installation and reset, and rejected alternatives (FR-007, FR-013, FR-056, FR-059).
2. **Message catalog and schemas**: message types, envelope, schema format and versioning, destinations, routing keys, consumer groups, dead-letter destinations, and owning producers (FR-008, FR-009, FR-012, FR-014).
3. **Outbox relays and consumers**: outbox design in each service, relay scheduling and batching, publish confirmation, idempotency records, acknowledgement timing, retry limits, and alarms (FR-010, FR-011, FR-013, FR-038).
4. **Saga over domain events**: the choreographed event sequence, steps, local transactions, persisted states, republish schedule, how an outcome event reaches the waiting request in a single Transaction Service process, deadlines, bounded waiting without held connections or locks, and how background completions are counted and minimized (FR-031–FR-046, FR-061, FR-066).
5. **Catalog replication**: initial load, full-catalog retrieval for a new or outdated consumer, version and ordering rules, staleness bound, and behavior during outages (FR-017–FR-020).
6. **Analytics delivery and ordering**: routing key, position-assignment rule, deduplication, durable consumption progress, checkpoints, and the visibility delay (FR-047–FR-055).
7. **Migration path**: incremental replacement of each synchronous interface, the test-replacement record, removal of the synchronous interfaces, and the revised architecture test (FR-006, FR-065).
8. **Evidence**: the reproducible baseline including the broker, report locations, the comparison with [Prior reports], and the architecture diagram's format and location (FR-059, FR-061–FR-063).
