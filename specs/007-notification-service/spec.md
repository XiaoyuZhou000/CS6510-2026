# Feature Specification: Sale Notification Service with Subscriber Reconciliation

**Feature Branch**: `feature/notification-service`

**Created**: 2026-11-11

**Status**: Draft

**Input**: User description: "Register all subscribers before the main load test and keep subscriptions unchanged during that test so sales and notifications can be reconciled reliably. Test subscription creation, updates, and cancellation in a separate scenario. A change takes effect when the Notification Service's database transaction commits. The service must record which subscribers were selected for each sale."

## Baseline and Sources

- **[C-I]–[C-XI]** refer to the principles of `.specify/memory/constitution.md` v7.0.0, amended on 2026-11-11. That amendment adds Notification Service as a fourth service, defines completed sales and the SaleCompleted event (Principle X), and sets the subscription and delivery guarantees (Principle XI). Together with the user description above, it is the feature description for this specification.
- **[Contract]** refers to `spec/self-checkout-openapi.yaml`, which stays authoritative and unchanged for the seven existing routes.
- **[Preserved]** refers to the feature 006 implementation on the `event-based-architecture` branch, its automated tests, and specifications `specs/001-*` through `specs/006-*`. `specs/006-event-based-architecture` is historical: where this specification differs from it, this specification governs, and feature 006's documents are not edited.
- **[Client]** refers to the unchanged Load Client in `load-client/src/`.
- **[Prior reports]** refers to the feature 006 benchmark evidence in `load-client/reports/event-based-architecture/`.

Today, three services (Transaction, Inventory, and Analytics) sit behind one gateway URL. Each owns its own database, and they communicate only through domain events on a single durable message broker, with every message written to its producer's outbox first. A checkout becomes COMPLETED only after Inventory has deducted stock. Nothing tells anyone that a sale happened. [Preserved]

This feature adds a fourth, independently deployable **Notification Service**. People or systems register as **subscribers** and choose the SKUs they care about. Each time a sale completes, every subscriber of a sold SKU receives exactly one notification for that SKU, carrying the quantity sold. Transaction Service only records that the sale happened. Notification Service alone decides who is told, records that decision, and delivers through each subscriber's own channel. A separate **subscriber test application** plays up to 100 or more subscribers and records what they receive, so that sales, subscriptions, deliveries, and receipts can be reconciled independently after every run. [C-I][C-X][C-XI][C-IX]

All user stories are required for acceptance. Priorities set delivery order, not optionality.

## Clarifications

### Session 2026-11-11

- Q: What is the difference between a delivered notification and a received one, and where does end-to-end latency end? → A: Delivered means the subscriber's channel acknowledged a durable publish of the notification. Received means the subscriber test application recorded it. Both are tracked and reported separately, and end-to-end latency ends at receipt. (FR-016, FR-021, FR-025, FR-026, FR-028)
- Q: How is subscriber isolation tested, and against what baseline is the other subscribers' latency compared? → A: One subscriber's channel is made to reject or stall publishes; stopping only that subscriber's thread does not count, because its channel still accepts publishes. The other 99 subscribers' p95 latency is compared with healthy runs that use the same workload and subscription seed, using the median of at least three runs of each kind. (FR-038, SC-006)
- Q: Is the 60-second bound a latency target or a drain deadline? → A: A drain deadline: every owed notification must be recorded as received within 60 seconds after the run's final completed sale. Latency is still reported from each sale's own completion time. (SC-007)
- Q: Where are notification results reported, given that the Load Client's JSON reports cannot observe subscriber receipt? → A: The Load Client's two JSON reports are kept unchanged. Each run also produces a separate notification reconciliation and latency report. (FR-028, FR-029)

## Recipient Selection Rule

This section defines the rule that Principle XI requires the specification to state.

1. **A subscription change takes effect when Notification Service's database transaction for that change commits.** A change that fails or rolls back never takes effect anywhere, including in any in-memory lookup structure.
2. **Recipients are selected once per sale, when Notification Service processes that sale.** The recipients of a sale are exactly the subscribers whose subscriptions, as committed at the moment of selection, include at least one SKU sold in that sale.
3. **Every selection reflects one consistent subscription state.** All committed subscription changes form a single ordered history, and each selection uses the state at exactly one point in that history: it includes every change committed before that point and none committed after it.
4. **The selection is durable and recorded.** Notification Service records, for each sale, which subscribers were selected for which SKUs and which point in the subscription history the selection used. It records this in the same local transaction that accepts the sale for processing.
5. **A recorded selection is final.** Notifications it creates are owed and delivered even if the subscriber later changes or cancels a subscription. Later changes affect only sales selected after those changes commit.
6. **Causality is respected.** A sale cannot be selected before it completes. Therefore, if a subscription change was confirmed as committed before the request that completed a sale was sent, that sale's selection includes the change. Changes that commit while a sale is being completed or selected may fall on either side, but rule 3 always holds.

**Main load test**: every subscriber is registered and confirmed before the Load Client starts, and no subscription changes until the run has drained. Every sale in the run is therefore selected against the same, fully known subscription state, which the subscriber test application records independently. Subscription creation, update, and cancellation are tested in a separate scenario (User Story 4) that checks rules 1–6 directly.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Record Every Completed Sale Exactly Once (Priority: P1)

As the store, each checkout that durably reaches COMPLETED produces exactly one completed-sale record, with a stable sale identity and the quantity of each SKU sold, written together with the completion itself. Nothing else produces one. [C-V][C-VI][C-X]

**Why this priority**: Every notification is derived from this record. If a sale is missing, duplicated, or wrong here, no later step can be correct.

**Independent Test**: With Notification Service stopped, complete a set of known baskets, including multi-unit lines, multi-SKU baskets, insufficient-stock rejections, repeated Complete requests, and checkouts that complete in the background after a `500 INTERNAL_ERROR` response. Compare the completed-sale events that Transaction Service published with its COMPLETED transactions and their basket lines.

**Acceptance Scenarios**:

1. **Given** an OPEN transaction holding three units of SKU A and one unit of SKU B, **When** completion succeeds, **Then** exactly one completed-sale event exists for that transaction, with quantity 3 for A and 1 for B, and it was recorded in the same local transaction that made the transaction COMPLETED.
2. **Given** a transaction whose checkout is rejected for insufficient stock and later completes on a second attempt, **When** events are inspected, **Then** exactly one completed-sale event exists, belonging to the successful attempt.
3. **Given** a completed transaction, **When** Complete is requested again, or the stock outcome that completed it is redelivered, or Transaction Service restarts, **Then** no additional completed-sale event is recorded and the sale's identity is unchanged.
4. **Given** accepted scans, pending attempts, and rejected checkouts, **When** events are inspected, **Then** none of them produced a completed-sale event.
5. **Given** a checkout that completed after its client received `500 INTERNAL_ERROR`, **When** events are inspected, **Then** it has exactly one completed-sale event, like any other completed sale.
6. **Given** Transaction Service commits a completion while the broker is unavailable, **When** the broker returns, **Then** the completed-sale event is published from Transaction Service's outbox, and the checkout's response and receipt are unaffected.
7. **Given** any completed-sale event, **When** its contents are inspected, **Then** it carries no recipient, subscriber, or subscription information.

---

### User Story 2 - Notify Each Subscriber Once per Sold SKU (Priority: P1)

As a subscriber of one or more SKUs, I receive one notification through my own channel for each sold SKU I subscribe to in each completed sale, carrying the sale's identity, the SKU, and the quantity sold. I receive nothing for SKUs I do not subscribe to. [C-XI]

**Why this priority**: This is the feature's purpose and the behavior every other story protects.

**Independent Test**: Register a handful of subscribers with known, overlapping SKU sets. Complete known baskets. Compare each subscriber's received notifications with the expected set computed from the baskets and subscriptions.

**Acceptance Scenarios**:

1. **Given** subscriber S1 subscribes to SKUs A and B and subscriber S2 subscribes to B only, **When** a sale of 3×A, 1×B, and 2×C completes, **Then** S1 receives exactly two notifications (A with quantity 3, B with quantity 1), S2 receives exactly one (B with quantity 1), and no one receives a notification for C.
2. **Given** a sale contains no SKU that anyone subscribes to, **When** it is processed, **Then** no notification is created, and the recorded selection for that sale shows no recipients.
3. **Given** a completed sale, **When** Notification Service processes it, **Then** it records which subscribers were selected for which SKUs, together with the point in the subscription history that it used, before treating the sale as accepted.
4. **Given** two sales of the same SKU, **When** both complete, **Then** each subscriber of that SKU receives one notification per sale, distinguishable by sale identity.
5. **Given** one subscriber's channel rejects or stalls publishes, **When** sales continue, **Then** other subscribers keep receiving their notifications without waiting for it, and the affected subscriber's notifications stay undelivered, retrying or parked, until its channel recovers and they are delivered.
6. **Given** any notification, **When** it is received, **Then** it carries a notification identity that is the same for every delivery of the same sale, SKU, and subscriber.

---

### User Story 3 - Reconcile Sales and Notifications Under Load With Fixed Subscriptions (Priority: P1)

As an evaluator, I register all subscribers through the subscriber test application before the main load test, run the unchanged Load Client with no subscription changes, let the system drain, and get a reconciliation report proving that every owed notification was delivered and received exactly once, with the correct quantity. [C-IX][C-XI]

**Why this priority**: The constitution makes this independent reconciliation the acceptance gate for notification correctness. Fixing subscriptions for the run turns "who should have been told" into a known set that does not depend on timing.

**Independent Test**: From a fresh baseline, start the subscriber test application with 100 subscriber threads and a recorded random seed, wait until it confirms every registration, run the unchanged Load Client, wait for the drain conditions, and run the reconciliation. Repeat with a deliberately injected fault, such as one removed delivery record or one duplicated receipt, and confirm that the reconciliation fails and names the problem.

**Acceptance Scenarios**:

1. **Given** a fresh baseline and a recorded seed, **When** the subscriber test application starts with N subscriber threads, **Then** it registers N subscribers whose SKU assignments are fully determined by the seed and its settings, and it confirms that Notification Service committed every registration before reporting itself ready.
2. **Given** the same seed and settings, **When** the subscriber test application is started again from a fresh baseline, **Then** every subscriber receives the same identity and the same SKU assignment.
3. **Given** all subscribers are registered and confirmed, **When** the unchanged Load Client runs, **Then** no subscription change occurs from the start of the run until the drain completes, and the reconciliation verifies this.
4. **Given** the run has finished and the system has drained, **When** the reconciliation runs, **Then** it independently compares completed sales, the registered subscription snapshot, Notification Service's selection and delivery records, and the subscriber receipt records, and it reports zero missing, zero unexpected, zero duplicate notifications, and zero quantity mismatches.
5. **Given** any mismatch, such as an owed notification absent from receipts, a receipt for a SKU the subscriber does not hold, two distinct effective notifications for one sale, SKU, and subscriber, or a wrong quantity, **When** the reconciliation runs, **Then** it reports each mismatch by category with the sale, SKU, and subscriber involved, and it fails.
6. **Given** a notification was delivered to a subscriber more than once with the same notification identity, **When** the reconciliation runs, **Then** it reports the redelivery separately and does not count it as a duplicate effective notification.
7. **Given** checkouts that completed in the background after `500 INTERNAL_ERROR`, **When** the reconciliation runs, **Then** those sales are owed notifications like any other, because completed sales are taken from durable state rather than from the Load Client's counts.

---

### User Story 4 - Create, Update, and Cancel Subscriptions With Commit-Time Effect (Priority: P2)

As a subscriber, I can register with one or more SKUs, add or remove SKUs, and cancel. Each change applies to sales selected after it commits, never to sales selected before it, and never at all if it fails. This is tested in its own scenario, separate from the main load test. [C-II][C-XI]

**Why this priority**: Subscription management is required, but mixing it into the main load test would make reconciliation depend on timing. A dedicated scenario tests the Recipient Selection Rule directly.

**Independent Test**: In a separate scenario, with sales running, create, update, and cancel subscriptions, including changes confirmed before a sale's Complete request, changes made while sales are in flight, and changes forced to fail. Verify each recorded selection against the subscription history and the rule.

**Acceptance Scenarios**:

1. **Given** a new subscriber registers for SKU A and the registration is confirmed, **When** a sale of A is then completed, **Then** the subscriber is selected for and receives a notification for A.
2. **Given** a subscriber adds SKU B and the change is confirmed, **When** a sale of B is then completed, **Then** the subscriber is selected for B. **Given** the subscriber then removes B and the change is confirmed, **When** another sale of B completes, **Then** the subscriber is not selected for B.
3. **Given** a subscriber cancels and the cancellation is confirmed, **When** later sales of its former SKUs complete, **Then** it is not selected and receives nothing for those sales.
4. **Given** a sale was already selected with the subscriber as a recipient, **When** the subscriber cancels or removes that SKU afterwards, **Then** the already-selected notification is still delivered (rule 5).
5. **Given** a subscription change whose database update fails or is rolled back, **When** subsequent sales are selected, **Then** the change has no effect: selections, notifications, and any in-memory lookup reflect the state before the failed change, and the requester is told the change was not applied.
6. **Given** subscription changes committed while sales are being completed, **When** the scenario drains, **Then** every recorded selection equals the subscription state at the single point in the history it names, and that point includes every change confirmed before the sale's Complete request was sent.
7. **Given** Notification Service restarts, **When** it resumes, **Then** its subscriptions and any in-memory lookup are rebuilt only from committed data, and selections made after the restart follow the same rule.
8. **Given** a request to create a subscriber with no SKU, to subscribe to an unknown SKU, to reuse an active subscriber's identity with a different SKU set, or to change a subscriber that does not exist, **When** it is submitted, **Then** it is rejected with a defined error, and nothing changes.

---

### User Story 5 - Keep Notifications Exactly Once Through Duplicates, Failures, and Restarts (Priority: P2)

As the store, duplicate or redelivered events, failed deliveries, and restarts of Transaction Service, Notification Service, the broker, or the subscriber test application never lose an owed notification and never create a duplicate effective one. Failed deliveries stay visible and retryable. [C-III][C-VII][C-XI]

**Why this priority**: The broker delivers at least once, and the constitution requires restart and recovery coverage. Exactly-once effective notifications must survive both.

**Independent Test**: During a workload with fixed subscriptions, duplicate and redeliver completed-sale events, inject delivery failures into chosen subscribers' channels, and crash and restart Transaction Service, Notification Service, and the broker at each processing boundary: before the event is published, after it is published but before it is consumed, after selection is recorded but before delivery, and after delivery but before the delivery record is updated. Also restart the subscriber test application. Drain and reconcile.

**Acceptance Scenarios**:

1. **Given** a completed-sale event is delivered to Notification Service twice, including after a crash before acknowledgement, **When** it is processed, **Then** one selection is recorded and each owed notification exists once.
2. **Given** Notification Service crashes after recording a selection but before delivering every notification, **When** it restarts, **Then** it delivers the remaining notifications from its durable records, and no notification gets a second effective instance.
3. **Given** delivery to a subscriber's channel fails, **When** the failure persists, **Then** the notification is retried on a bounded, documented schedule, its failure is visible to operators, and after the retry limit it is parked rather than dropped. **When** an operator retries it, **Then** it is delivered with its original identity.
4. **Given** the broker restarts while completed-sale events or notifications are in flight, **When** it returns, **Then** no confirmed event or delivery is lost, and every owed notification is eventually received exactly once in effect.
5. **Given** Transaction Service restarts with completed-sale events still unpublished, **When** it resumes, **Then** it publishes them from its outbox and Notification Service processes each sale once.
6. **Given** the subscriber test application stops and later restarts, **When** it resumes, **Then** notifications delivered while it was stopped are still received, its receipt records from before the stop are kept, and the reconciliation still passes.
7. **Given** a completed-sale event cannot be processed after the bounded number of retries, for example because it is malformed, **When** it is parked, **Then** it is visible to operators, an alarm is raised, the reconciliation reports that sale's notifications as missing, and other sales keep flowing.

---

### User Story 6 - Keep Checkout, Analytics, and the Client Interface Unchanged (Priority: P2)

As a station or evaluator, I use the same base URL and seven routes, with the unchanged Load Client, and checkout and analytics behave exactly as before, whether Notification Service is healthy, slow, backlogged, or stopped. [Contract][Client][C-IV][C-VII]

**Why this priority**: The constitution forbids notification work from weakening any existing guarantee or blocking the checkout path.

**Independent Test**: Run the existing contract tests and feature 006's automated suite with Notification Service running. Then stop Notification Service, and separately make all subscriber channels fail, while the Load Client runs. Verify the seven routes, stock reconciliation, and analytics windows.

**Acceptance Scenarios**:

1. **Given** all four services and the broker are running, **When** the contract tests and the unchanged Load Client use the base URL, **Then** every existing route matches the established contract and the Load Client needs no change.
2. **Given** Notification Service is stopped, slow, or has a large backlog, **When** stations scan and complete, **Then** Start, Scan, Lookup, and Complete behave as in feature 006, Complete never waits for notification work, and completed-sale events wait durably until Notification Service returns.
3. **Given** notification traffic of any volume, **When** popular items are computed, **Then** windows and rankings are identical to those computed from the same accepted scans without Notification Service.
4. **Given** any number of sales and notifications, **When** stock is reconciled after settlement, **Then** initial stock minus final stock equals the units in COMPLETED transactions, as before.

---

### User Story 7 - Operate and Reset Notification Service Independently (Priority: P2)

As a maintainer, I build, start, stop, restart, reset, and seed Notification Service on its own, it can reach only its own data, and one documented baseline procedure resets all four services, the broker, and the subscriber test application's records together. [C-I][C-II][C-VIII]

**Why this priority**: Independent deployment and a reproducible baseline are prerequisites for trustworthy reconciliation and benchmark evidence.

**Independent Test**: Build and start Notification Service alone with the broker running. Attempt cross-service data access with each service's credentials. Run the baseline procedure and inspect every service, the broker, and the subscriber test application's records. Restart Notification Service during a workload.

**Acceptance Scenarios**:

1. **Given** only Notification Service's artifacts and configuration plus a running broker and database, **When** it is built and started, **Then** it runs without building or starting another service.
2. **Given** Notification Service's credentials, **When** they are used to read or change another service's data, or another service's credentials are used against Notification Service's data, **Then** access is denied.
3. **Given** the documented baseline procedure, **When** it runs, **Then** feature 006's baseline holds, Notification Service holds no subscribers, selections, or delivery records, the subscriber test application holds no receipt records from an earlier run, no undelivered event or notification from an earlier run remains, and no service wrote another service's data.
4. **Given** a workload is running, **When** Notification Service is restarted, **Then** the other services keep serving as specified in *Availability and Failure Behavior*, and after the restart the reconciliation passes.

---

### User Story 8 - Produce Notification Performance Evidence and the Updated Diagram (Priority: P3)

As an evaluator, I find genuine performance evidence for notification delivery at several subscriber counts up to 100, the two standard Load Client reports produced against the four-service deployment, and an architecture diagram that shows Notification Service, its database, the subscriber test application, and every message flow. [C-IX]

**Why this priority**: Performance evidence is meaningful only after correctness is established.

**Independent Test**: For each subscriber count, reset to a fresh baseline, register subscribers with a recorded seed, run the unchanged Load Client with the same settings, drain, reconcile, and collect the metrics. Separately produce the default and stress reports. Compare the diagram with the message catalog.

**Acceptance Scenarios**:

1. **Given** at least three subscriber counts including 100, each from a fresh baseline with the same Load Client settings, **When** each run drains, **Then** its notification report gives completed sales per second, notifications per second, end-to-end notification latency (p50, p95, p99), delivery failures, and the maximum and final backlog or consumer lag, together with the seed and settings, and each run's reconciliation passes.
2. **Given** a fresh baseline with 100 subscribers registered, **When** `java -cp out Main` runs and, separately after another fresh baseline, `java -cp out Main --stations=100 --duration=120` runs, **Then** each produces a genuine, unmodified Load Client JSON report that is committed and compared with [Prior reports], plus a separate notification report whose reconciliation passes.
3. **Given** the committed architecture diagram, **When** it is compared with the deployment and the message catalog, **Then** it shows the API Gateway, the four services, each service's database, the broker with every topic or queue and its producers and consumers, and the subscriber test application with its channels, and it contains no synchronous service-to-service link.

### Edge Cases

**Sales and events**

- A stock outcome that completes a transaction is redelivered after the transaction is already COMPLETED. No second completed-sale event is recorded. [FR-003]
- A completion commits but Transaction Service crashes before its outbox relay publishes the event. The event is published after restart. [FR-002]
- A basket line holds many units of one SKU. The sale carries one quantity for that SKU, and each subscriber gets one notification with that quantity, not one per unit. [FR-001][FR-013]
- A sale's event reaches Notification Service long after completion, for example after an outage. Selection happens on processing, under the rule, and latency evidence reflects the delay. [FR-010]

**Subscriptions**

- Two changes to the same subscriber race. Both are serialized in the subscription history, the final state reflects their commit order, and every selection reflects one point in that order. [FR-007]
- A subscriber removes its last SKU. The request is rejected; cancellation is the way to stop all notifications. [FR-006]
- A cancelled subscriber registers again under the same identity. It is treated as a new registration from its commit point and receives nothing for sales selected while it was cancelled. [FR-006]
- A subscription change commits but its confirmation is lost. Repeating the same request leaves the same final state and creates no second subscriber. [FR-006]
- Notification Service runs more than one process. Every selection still reflects one committed subscription state, and no process's in-memory lookup is authoritative. [FR-008]

**Delivery**

- A notification is delivered but the delivery record update is lost in a crash. It is delivered again with the same identity, the subscriber records a redelivery, and the reconciliation does not count a duplicate. [FR-015][FR-021]
- A subscriber's channel rejects or stalls publishes for longer than the retry policy. Its notifications are parked, visible, and retryable; other subscribers are unaffected. [FR-016][FR-017]
- The subscriber test application stops while its channels stay available. Notifications are still delivered, but they are not received until it returns; the reconciliation reports them as delivered but not received if it is still down when the drain bound expires. [FR-016][FR-021][FR-026]
- The subscriber test application receives a notification for an identity it does not know. It records it, and the reconciliation reports it as unexpected. [FR-026]
- Clocks of different processes differ. Latency is measured on one machine with one clock; no correctness decision compares clocks across services. [FR-028]

**Verification**

- The Load Client's `totalTransactions` differs from the number of COMPLETED transactions because of background completions. Reconciliation uses durable completed sales. [FR-024]
- The drain conditions are never met, for example because an event is parked. The reconciliation still runs after a bounded wait and fails, naming the missing notifications. [FR-025]

## Requirements *(mandatory)*

### Functional Requirements

**Completed sales**

- **FR-001 [C-VI][C-X]**: A sale MUST exist only once a checkout has durably reached COMPLETED in Transaction Service after a successful stock deduction. Each completed transaction is exactly one sale. The sale's quantity for a SKU is the number of units of that SKU in the completed basket.
- **FR-002 [C-VII][C-X]**: Transaction Service MUST record a versioned completed-sale event in the same local database transaction that makes the checkout COMPLETED, and MUST publish it only through its outbox. The event MUST carry a stable sale identity, the sold SKUs with their quantities, and the sale's completion time, in addition to the standard message envelope. [C-III]
- **FR-003 [C-V][C-X]**: Exactly one effective completed-sale event MUST exist per sale. Scans, pending attempts, rejected checkouts, Saga retries, redelivered stock outcomes, repeated Complete requests, and restarts MUST NOT record another one or change the sale's identity.
- **FR-004 [C-I][C-X]**: The completed-sale event MUST NOT carry recipients, subscription data, or any decision about whom to notify. Transaction Service MUST NOT read subscription data or record whom to notify.
- **FR-005 [C-V][C-VII]**: Checkout completion MUST NOT wait for, depend on, or be compensated because of notification processing. Complete's response time, outcome, and receipt MUST be unaffected by Notification Service's state.

**Subscribers and subscriptions**

- **FR-006 [C-XI]**: Notification Service MUST let a subscriber register under a stable identity with one or more SKUs, add SKUs, remove SKUs while keeping at least one, cancel all its subscriptions, and register again after cancelling. It MUST reject a registration with no SKU, an unknown SKU, a registration that reuses an active subscriber's identity with a different SKU set, and a change to a subscriber that does not exist, with a defined error and no change. Repeating an identical request, for example after its confirmation was lost, MUST return the original result without changing state.
- **FR-007 [C-II][C-XI]**: A subscription change MUST take effect only when Notification Service's database transaction for it commits, and MUST be confirmed to the requester only after that commit. A failed or rolled-back change MUST NOT take effect. All committed changes MUST form a single ordered subscription history that Notification Service records durably.
- **FR-008 [C-II][C-XI]**: Any in-memory SKU-to-subscriber lookup MUST be derived only from committed subscription data, MUST be rebuilt from that data after a restart, MUST never reflect a failed change, and MUST NOT be the authority for a selection that disagrees with committed data. With more than one Notification Service process, every selection MUST still satisfy FR-010.
- **FR-009 [C-XI]**: Notification Service MUST support at least 100 active subscribers, each with its own independent asynchronous channel, so that a slow, failed, or stopped subscriber does not delay delivery to any other subscriber. Subscriber channels MUST NOT introduce a second broker or other messaging middleware. [C-VIII]

**Recipient selection**

- **FR-010 [C-XI]**: For each sale, Notification Service MUST select recipients once, according to the *Recipient Selection Rule*: the selection MUST equal the committed subscription state at exactly one point in the subscription history, and MUST include every change confirmed before the request that completed the sale was sent.
- **FR-011 [C-XI]**: Notification Service MUST durably record, for each sale, the selected subscribers per SKU and the point in the subscription history used, including sales with no recipients. It MUST record this in the same local transaction that accepts the sale for processing, and MUST acknowledge the completed-sale event only after that commit. [C-III]
- **FR-012 [C-XI]**: A recorded selection MUST be final. Notifications it creates MUST be delivered regardless of later subscription changes, and a repeated or redelivered event for the same sale MUST NOT create a new selection.

**Notification delivery**

- **FR-013 [C-XI]**: For each sale, each selected subscriber MUST receive exactly one effective notification per sold SKU it was selected for, carrying the sale identity, the SKU, the quantity sold of that SKU in that sale, the notification identity, and the sale's completion time. No notification may be created for a SKU the subscriber was not selected for.
- **FR-014 [C-XI]**: Each notification MUST have an identity that is stable for its sale, SKU, and subscriber. Every delivery attempt of the same notification MUST carry that identity.
- **FR-015 [C-III][C-XI]**: Broker and channel delivery MAY be at least once. Duplicate or redelivered events, retries, and restarts of any service, the broker, or the subscriber test application MUST NOT create a second effective notification for the same sale, SKU, and subscriber.
- **FR-016 [C-XI]**: Each notification MUST have a durable delivery record in Notification Service, created with the selection (FR-011), that shows its state: pending, delivered, retrying with its attempt count and last failure, or parked. A notification is **delivered** when its subscriber's channel has acknowledged a durable publish of it, and the delivery record keeps that time. Delivery is tracked by Notification Service and is distinct from receipt (FR-021).
- **FR-017 [C-III][C-XI]**: Failed delivery MUST be retried on a bounded, documented schedule. After the retry limit, the notification MUST be parked, MUST be visible to operators with its failure reason, MUST raise an alarm, and MUST be retryable on operator request with its original identity. No notification may be silently dropped, and a parked notification MUST NOT block other subscribers.
- **FR-018 [C-III]**: A completed-sale event that cannot be processed after the bounded number of retries MUST be parked, MUST be visible to operators, MUST raise an alarm, and MUST NOT block other sales.

**Subscriber test application**

- **FR-019 [C-IX]**: A separate subscriber test application, outside every service, MUST run a configurable number of subscriber threads, each acting as one subscriber with its own channel. It MUST register its subscribers through Notification Service's subscription interface and MUST report itself ready only after every registration is confirmed.
- **FR-020 [C-IX]**: Subscriber identities and SKU assignments MUST be derived reproducibly from a recorded random seed and recorded settings, including the number of subscribers and the number or range of SKUs per subscriber, drawn from the baseline catalog. The same seed and settings MUST always produce the same identities and assignments, and the application MUST record the resulting subscription snapshot in its own output.
- **FR-021 [C-IX]**: The subscriber test application MUST write a structured, durable record of every received notification, including redeliveries, with the subscriber, sale identity, SKU, quantity, notification identity, sale completion time, and receipt time. A notification is **received** when this record is written. Records from before a restart MUST be kept.
- **FR-022 [C-IX][C-XI]**: For the separate subscription-change scenario, the subscriber test application or a companion test driver MUST be able to create, update, and cancel subscriptions at controlled points during sales, and MUST record each change's request time and confirmation.

**Main load test and reconciliation**

- **FR-023 [C-IX]**: In the main load test, all subscribers MUST be registered and confirmed before the Load Client starts, and no subscription change may occur until the drain completes. The reconciliation MUST verify, from Notification Service's subscription history, that no change committed during that period.
- **FR-024 [C-IX]**: After the drain, the reconciliation MUST independently obtain four sources: completed sales and their SKU quantities from Transaction Service's durable state; the subscription snapshot recorded by the subscriber test application; Notification Service's selection and delivery records; and the subscriber receipt records. Each source MUST be obtained through its owner's own interfaces or operational means, never by one service reading another's data. [C-II]
- **FR-025 [C-IX]**: The drain is complete when Transaction Service has no unpublished completed-sale events, Notification Service has consumed every published completed-sale event and has no pending or retrying delivery, and the subscriber test application has received every delivered notification. The reconciliation MUST wait for the drain for a bounded, documented time and MUST fail if the drain does not complete, reporting what is outstanding.
- **FR-026 [C-IX]**: The reconciliation MUST compute the owed notifications from completed sales and the applicable snapshot, and compare them with Notification Service's selections, its delivery records, and the receipts. It MUST report delivered and received counts separately; report missing, unexpected, and duplicate effective notifications and quantity mismatches, each with the sale, SKU, and subscriber, splitting missing notifications into not delivered and delivered but not received; report redeliveries and parked notifications separately; and fail on any mismatch, on any parked notification or event, and on any disagreement between Notification Service's recorded selections and the snapshot.
- **FR-027 [C-IX][C-XI]**: In the subscription-change scenario, the reconciliation MUST use Notification Service's recorded selections as the applicable snapshot only after verifying each one against FR-010: it equals the subscription state at the point it names, and that point includes every change confirmed before the sale's Complete request was sent. It MUST also verify that failed changes never appeared in any selection. Any violation fails the scenario.

**Performance evidence**

- **FR-028 [C-IX]**: Notification performance MUST be measured with the unchanged Load Client generating sales, at no fewer than three distinct subscriber counts including 100, each from a fresh baseline with the same Load Client settings and a recorded seed. Each run MUST produce a notification report, separate from the Load Client's JSON report, that gives completed sales per second, notifications delivered and received per second, end-to-end notification latency (p50, p95, p99) from each sale's own completion time to each notification's receipt time, delivery failures, the maximum and final backlog or consumer lag, the drain time after the final completed sale, the seed and settings, and the reconciliation result, which MUST pass. Results MUST NOT be fabricated.
- **FR-029 [C-IX]**: Two genuine JSON reports MUST be produced by the unchanged Load Client against the four-service deployment with 100 subscribers registered, one from `java -cp out Main` and one from `java -cp out Main --stations=100 --duration=120`, each after a separate fresh baseline, committed unmodified, compared with [Prior reports], and followed by feature 006's invariant checks. Because those reports cannot observe receipt, each run MUST also commit its own notification report (FR-028) with a passing reconciliation.
- **FR-030 [C-VIII][C-IX]**: Notification Service MUST provide documented operational means, outside the public REST contract, to report its consumption backlog, pending, retrying, and parked notifications, parked events, alarms, and the delivery counts and latencies that FR-028 requires.

**Preserved behavior and external interface**

- **FR-031 [Contract][Client][C-IV]**: The seven existing routes, `spec/self-checkout-openapi.yaml`, and the Load Client MUST remain unchanged. Every feature 006 requirement on checkout, inventory, analytics, outboxes, replication, availability, and recovery MUST continue to hold, and feature 006's automated tests MUST pass without weakened assertions.
- **FR-032 [C-IV]**: Notification Service's subscription and operational endpoints MUST be purely additive, reachable through the same client-facing base URL under paths that do not overlap any existing route, and documented in a versioned contract.
- **FR-033 [C-VII]**: Notification Service's state, backlog, or unavailability MUST NOT block Start, Scan, Lookup, or Complete, and MUST NOT change analytics inputs or results.

**Service boundaries and operations**

- **FR-034 [C-I][C-II]**: Notification Service MUST be independently buildable, startable, stoppable, restartable, and deployable, with its own runtime, entry point, configuration, port, database, and credentials limited to its own data. It MUST communicate with other services only through broker events.
- **FR-035 [C-VIII]**: Notification Service, its channels, and the subscriber test application MUST document build or installation, startup, shutdown, ports, configuration, storage, destinations, and dependencies. Restarting Notification Service, Transaction Service, the broker, or the subscriber test application MUST preserve the reconciliation result.
- **FR-036 [C-VIII][Architectural Constraints]**: One documented baseline procedure MUST reset all four services, the broker, and the subscriber test application's records, each through its own mechanism, producing feature 006's baseline plus an empty Notification Service and no receipt records.
- **FR-037 [C-IX]**: The architecture diagram MUST be updated to show the API Gateway, the four services, each service's database, the broker with every topic or queue and its producers and consumers, and the subscriber test application with its channels, and it MUST agree with the message catalog.

**Tests**

- **FR-038 [C-IX]**: Automated tests MUST cover:
  - exactly one completed-sale event per sale, including repeated completions, redelivered outcomes, rejected and pending attempts, background completions, and Transaction Service restarts;
  - recipient selection and per-SKU quantities for overlapping subscriptions and sales with no recipients;
  - concurrent sales with fixed subscriptions, reconciled end to end;
  - subscription creation, update, cancellation, re-registration, rejected requests, failed changes, and changes concurrent with sales, checked against the *Recipient Selection Rule*;
  - duplicate and redelivered completed-sale events and redelivered notifications;
  - delivery failure, retry, parking, and operator retry;
  - restarts of Transaction Service, Notification Service, the broker, and the subscriber test application at each processing boundary, with recovery to a fully reconciled state;
  - independence of subscriber channels, tested by making one subscriber's channel reject or stall publishes; stopping only a subscriber thread does not test isolation, because its channel still accepts publishes;
  - that the reconciliation detects each mismatch category when one is injected;
  - database isolation for Notification Service, the unchanged contract, and feature 006's suite.

### Data Ownership

| Information | Source of truth | Copies elsewhere | Consistency expectation |
|---|---|---|---|
| Completed sales and their SKU quantities | Transaction | Notification keeps the sale identity and quantities it received, for selection and delivery | Recorded with completion; reaches Notification eventually, at least once |
| Subscribers, subscriptions, and the subscription history | Notification | Notification's in-memory lookup; the subscriber test application's own snapshot for verification | Effective at commit; the lookup is derived and never authoritative |
| Selections and delivery records | Notification | None | Durable before the sale's event is acknowledged |
| Receipt records | Subscriber test application | None | Durable on receipt; includes redeliveries |
| Catalog, stock, transactions, analytics | As in feature 006 | As in feature 006 | Unchanged |

### Required Message Flows

The plan names event types, destinations, routing keys, and channel mechanisms. Feature 006's flows are unchanged.

| Flow | Producer | Consumer | Timing | Required contents | Idempotency key |
|---|---|---|---|---|---|
| Sale completed | Transaction | Notification | Recorded with COMPLETED, published through the outbox, at least once | Sale identity, sold SKUs with quantities, completion time | Sale identity |
| Notification to a subscriber | Notification | That subscriber's channel | After the selection commits, retried until delivered or parked | Notification identity, sale identity, SKU, quantity, completion time | Notification identity |

### Availability and Failure Behavior

Feature 006's table applies unchanged, with these additions:

| Condition | Required observable behavior |
|---|---|
| Notification Service unavailable | Its own endpoints return the gateway's existing unavailable response. All seven existing routes are unaffected. Completed-sale events wait durably and are processed after restart. |
| Notification Service's database unavailable | Subscription changes return an error without taking effect. Event consumption pauses without acknowledgement. Other services are unaffected. |
| A subscriber's channel rejects or stalls publishes | Its notifications stay undelivered, retry, and are parked after the retry limit. Other subscribers are unaffected. |
| The subscriber test application stopped while its channels stay available | Notifications are delivered and wait in their channels; they are received after it restarts. |
| Broker unavailable | Completed-sale events stay in Transaction's outbox. Delivery through broker-carried channels pauses and resumes when the broker returns. Checkout behaves as in feature 006. |

### Key Entities

- **Sale**: A completed transaction, identified by a stable sale identity, with its sold SKUs, quantities, and completion time. Owned by Transaction Service.
- **Completed-Sale Event**: The versioned message that announces one sale, recorded with the completion and published through the outbox. It carries no recipient information.
- **Subscriber**: A stable identity with an independent channel, active or cancelled. Owned by Notification Service.
- **Subscription**: A subscriber's interest in one SKU, effective from the commit of the change that created it until the commit of the change that removed it.
- **Subscription History**: The ordered record of committed subscription changes, used to identify the state that a selection applied.
- **Selection Record**: For one sale, the selected subscribers per SKU and the point in the subscription history used.
- **Notification**: One owed message for a sale, SKU, and subscriber, with a stable identity and quantity.
- **Delivery Record**: A notification's delivery state, attempts, failures, and timestamps, including when its channel acknowledged the durable publish.
- **Receipt Record**: The subscriber test application's record of one received notification, including redeliveries.
- **Subscription Snapshot**: The subscriber test application's seed-derived record of every registered subscriber and its SKUs for the main load test.
- **Reconciliation Report**: The result of comparing owed notifications with selections, deliveries, and receipts, listing every mismatch by category, with a pass or fail verdict.
- **Notification Report**: One run's notification evidence, kept separate from the Load Client's JSON report: the reconciliation report, the delivery and receipt metrics, latency percentiles, backlog or lag, drain time, seed, and settings.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001 [C-X]**: Across tests and every benchmark run, the number of effective completed-sale events equals the number of COMPLETED transactions exactly, and repeated completions, redeliveries, and restarts add zero events.
- **SC-002 [C-XI][C-IX]**: Every main load test run, at every subscriber count and in both standard reports, ends with a passing reconciliation: zero missing, zero unexpected, zero duplicate effective notifications, zero quantity mismatches, and zero parked notifications or events.
- **SC-003 [C-IX]**: Injecting one instance of each mismatch category into a copy of a passing run's records causes the reconciliation to fail and to name 100% of the injected mismatches.
- **SC-004 [C-XI]**: In the subscription-change scenario, 100% of recorded selections match the subscription state at the point they name, 100% include every change confirmed before the sale's Complete request, and zero failed changes appear in any selection or notification.
- **SC-005 [C-XI]**: Across fault-injection runs covering duplicate and redelivered events, delivery failures, and restarts of Transaction Service, Notification Service, the broker, and the subscriber test application at every processing boundary, 100% of owed notifications are eventually received, zero duplicate effective notifications occur, and every failed delivery is visible until it succeeds or is parked.
- **SC-006 [C-XI]**: With 100 subscribers, one subscriber's channel is made to reject or stall publishes for the whole run; stopping only that subscriber's thread does not satisfy this test. Using the same Load Client workload and subscription seed, the median over at least three such runs of the other 99 subscribers' p95 end-to-end latency stays within 20% of the median over at least three healthy runs of the same 99 subscribers. In each faulted run, the other 99 subscribers have zero missing or duplicate notifications, and after the fault is removed and parked notifications are retried, the full reconciliation passes.
- **SC-007 [C-XI][C-VIII]**: Drain deadline: with all components healthy and 100 subscribers, every owed notification of the default Load Client run is recorded as received within 60 seconds after the run's final completed sale, meaning the latest completion time among its durable completed sales, background completions included. Latency is still reported from each sale's own completion time, and the stress run reports its drain time.
- **SC-008 [C-IV][C-VII]**: With Notification Service stopped during a Load Client run, zero Start, Scan, Lookup, or Complete failures are attributable to it, and after it returns, the reconciliation passes.
- **SC-009 [Contract][Client]**: 100% of the existing contract tests and feature 006's automated suite pass, and the Load Client source and the OpenAPI document are byte-identical to those of feature 006.
- **SC-010 [C-IX]**: Committed notification reports cover at least three subscriber counts including 100, each reporting completed sales per second, notifications per second, p50, p95, and p99 latency, delivery failures, and backlog or lag, with its seed and settings, and each with a passing reconciliation.
- **SC-011 [C-IX]**: Two genuine, committed JSON reports from the four-service deployment with 100 subscribers, one default and one 100-station, 120-second, each follow a fresh baseline, are left unmodified, include a comparison with [Prior reports], pass feature 006's invariant checks, and are each accompanied by a committed notification report with a passing reconciliation. The default run records zero errors for each Load Client operation.
- **SC-012 [C-I][C-II]**: Notification Service builds, starts, stops, and restarts on its own, and 100% of cross-service data access attempts involving its credentials or data are denied.
- **SC-013 [C-IX]**: Running the subscriber test application twice with the same seed and settings produces identical subscriber identities and SKU assignments.
- **SC-014 [C-IX]**: The committed architecture diagram shows 100% of the services, databases, topics or queues, channels, and flows in the message catalog, and nothing that contradicts it.

## Assumptions

- Subscribers are test identities created by the subscriber test application or test drivers. Authentication, authorization, personal contact data, and real email, SMS, or push delivery are out of scope.
- Delivered and received are distinct, separately tracked states (FR-016, FR-021). End-to-end latency runs from each sale's own completion time to each notification's receipt time.
- No ordering is guaranteed among notifications to the same subscriber. Reconciliation does not depend on order.
- Cancellation ends all of a subscriber's subscriptions. Removing a single SKU is an update, and a subscriber cannot hold zero SKUs while active.
- SKU assignments are drawn from the 2,000-item baseline catalog. The plan chooses the default number of SKUs per subscriber and the drawing method so that notification volume is meaningful, given that the Load Client picks SKUs with a skewed popularity distribution.
- All components run on one machine for benchmarks and tests, so timestamps from different processes share one clock for latency measurement. Correctness never depends on comparing clocks.
- Benchmarks run one process per service. Correctness guarantees must also hold with more than one Notification Service process.
- The subscriber counts used for performance evidence, beyond the required minimum of three including 100, and the Load Client settings used for those runs are chosen in the plan and recorded with the evidence.
- No numerical throughput target applies. Checkout throughput with Notification Service running is measured and compared with [Prior reports], and correctness is never traded for throughput.
- Presentation materials, video production, and a separate logging service are out of scope.

## Constitution Alignment

This specification implements constitution v7.0.0 and weakens none of its MUST requirements. The Recipient Selection Rule is the definition that Principle XI requires the specification to provide. Feature 006's documents stay unedited as history [Preserved]. The v7.0.0 amendment is uncommitted in the working tree; under Governance its Sync Impact Report must be removed before it is committed, and it should be committed before or together with this specification.

## Planning Obligations

The plan MUST address at least the following and trace each item to the listed requirements:

1. **Completed-sale event**: schema and version, sale identity, where it is recorded in Transaction Service's completion step, outbox relay, and destination (FR-001–FR-005).
2. **Subscription model**: interface and contract, validation, idempotent requests, the ordered subscription history, the in-memory lookup and its rebuild, and multi-process behavior (FR-006–FR-009, FR-032).
3. **Selection and delivery**: the selection transaction, how the history point is recorded, notification identity, delivery states, retry schedule, parking, operator retry, and alarms (FR-010–FR-018).
4. **Subscriber channels**: the channel mechanism on the existing broker, its independence per subscriber, and what counts as the channel's acknowledgement of a durable publish (FR-009, FR-016).
5. **Subscriber test application**: threads, seed handling, SKU assignment, readiness, receipt record format and storage, and the subscription-change driver (FR-019–FR-022).
6. **Reconciliation**: how each source is obtained, drain detection and its bound, mismatch categories, report format, and fault injection for SC-003 (FR-023–FR-027).
7. **Channel fault injection**: how one subscriber's channel is made to reject or stall publishes for SC-006, and the paired healthy and faulted runs (FR-038, SC-006).
8. **Evidence**: subscriber counts, Load Client settings, metric collection, the notification report format and location next to the Load Client's JSON reports, the baseline procedure, and the updated diagram (FR-028–FR-030, FR-036, FR-037).
