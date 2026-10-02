# Pipeline Analytics Code Walk — Speech Draft

Estimated delivery time: 16–18 minutes, plus questions.

## Title slide — Pipeline Analytics Code Walk

Hi everyone. Today I’m going to walk through the pipeline architecture we added to the self-checkout system.

I’ll follow one simple event through the design: a customer scans an item, the checkout operation succeeds, and that accepted scan continues into an asynchronous analytics pipeline. By the end of the pipeline, the system has produced and stored a deterministic ranking of the most frequently scanned items.

The important design goal is that analytics should not slow down or change the customer’s checkout result.

## Slide 1 — Pipeline where the data already flows

The first question was where a pipeline architecture actually fits.

The complete checkout workflow is not naturally a pipeline. Starting a transaction, adding items, and completing a purchase are related business operations, and the client expects a synchronous response from each one. Turning that entire path into an asynchronous pipeline would complicate the API and make transaction handling harder to reason about.

The analytics path is different. Every accepted scan passes through a predictable sequence of independent transformations. We collect scans into fixed-size windows, count and rank the SKUs in each window, and persist each completed ranking.

So we chose a narrow architectural boundary. The public checkout API stays synchronous, while only the windowed analytics work becomes asynchronous. This gives us a meaningful use of Pipes and Filters without forcing the pattern onto the rest of the application.

## Slide 2 — One synchronous boundary, three asynchronous stages

This diagram shows that boundary.

The load client sends an HTTP request. The API delegates to the transaction service, which validates the transaction and asks the basket to accept the scan. Everything up to that point remains on the synchronous request path.

After the scan has been accepted, the transaction service emits one `AcceptedScan` message. That message crosses into the analytics subsystem.

From there, three worker stages run asynchronously. The window filter groups accepted scans into overlapping windows. The ranking filter converts each window into a deterministic Top 10. The persistence filter writes the completed ranking to MySQL.

Three blocking queues connect those stages: the ingress queue, the window queue, and the ranking queue. Each queue is both a pipe and a concurrency boundary. A stage consumes one message type, performs one transformation, and publishes a new immutable message for the next stage.

The main composition point is in `Main`. It constructs the `AnalyticsService`, then injects `analytics::recordAcceptedScan` into the transaction service through the `AcceptedScanSink` interface. That keeps the transaction layer independent of the analytics implementation. The transaction service only knows that it can report a successfully accepted SKU.

The read path is separate from this graph. API reads go to the latest ranking that has already been committed to the database, not to any in-progress state inside the pipeline.

## Slide 3 — A successful basket scan emits one analytics message

Now let’s follow a concrete example. Suppose station 7 scans `SKU-42`.

The transaction service first asks the basket to add the scan. The basket checks that the transaction is still open and, if the scan is valid, updates its state and returns an accepted snapshot.

Only after that admission succeeds does the transaction service call `recordAcceptedScan` with `SKU-42`. The analytics service wraps the SKU in an immutable `AcceptedScan` message and offers it to the ingress queue.

The client then receives the normal synchronous scan response. The client does not wait for a 1,000-scan window, a ranking calculation, or a database write.

The ordering here matters. We never count a rejected scan because analytics receives the event only after the basket accepts it. At the same time, analytics is isolated from checkout. If the analytics call fails, the service logs the failure, but it does not roll back a scan that the basket has already accepted.

That is a deliberate consistency boundary. Checkout owns the customer-facing result. Analytics observes accepted activity and processes it independently.

## Slide 4 — Each filter performs one transformation

This is the Pipes and Filters pattern in its simplest form.

The window stage transforms individual accepted scans into immutable window snapshots. The ranking stage transforms a snapshot into an ordered Top 10. The persistence stage transforms that ranked message into one durable database commit.

The stages do not share mutable working data. They communicate only through messages on blocking queues. That reduces the amount of cross-thread coordination we need and makes each filter easier to test in isolation.

The queues are FIFO, so windows remain ordered as they move through the system. We also use explicit end messages for lifecycle control. Those messages travel through the same queues as the data, which lets every stage finish earlier work before it exits.

`AnalyticsService` acts as the facade and lifecycle owner. It creates the queues and workers, accepts new scan events, exposes the read operation, and coordinates shutdown. `PipelineMessage` defines sealed message families so each queue carries only the messages intended for that stage.

## Slide 5 — A ring buffer forms exact 1,000-scan windows

The window filter contains the main streaming algorithm.

Each window contains exactly 1,000 accepted scans. The slide interval is 500, so consecutive windows overlap by half. The first result covers scans 1 through 1,000. The next covers 501 through 1,500. The third covers 1,001 through 2,000.

We implement that with a fixed array of 1,000 slots. The `ringIndex` calculation maps each scan number to an array position. At scan 1,001, the index wraps back to slot zero and replaces the oldest value. This means memory use for active window data stays constant even as the total number of scans grows.

The second function decides when a complete window is ready. We do not emit anything before 1,000 scans. After that, we emit whenever the number of additional scans is divisible by 500. That gives us the exact boundaries shown in the table.

When a window becomes eligible, the filter reads the ring in chronological order and copies the 1,000 SKUs into an immutable list. The copy is important because the ring immediately continues changing as new scans arrive. The next stage must receive a stable snapshot rather than a view of mutable storage.

So the result combines overlapping windows with fixed memory and exact, testable boundary behavior.

## Slide 6 — Same window, same Top 10

The ranking filter is intentionally stateless.

For this small example, the window contains A, B, A, C, A, B. The filter first counts occurrences, which gives A three scans, B two, and C one. It then sorts the entries by scan count in descending order.

Count alone is not enough because two SKUs can have the same frequency. We therefore use the SKU in ascending order as the tie-breaker. That gives us a total ordering, so the same input window always produces the same ranked output.

This detail helps beyond presentation. Deterministic results make unit tests stable, make performance comparisons easier to reproduce, and avoid rankings that change just because a hash map happened to iterate in a different order.

The filter has no database dependency and keeps no state between messages. Given one immutable window, it produces one immutable ranking. That makes it a clean, independently testable transformation stage.

## Slide 7 — A ranking becomes visible only after one atomic commit

The persistence filter is where the pipeline result becomes durable and visible to readers.

For each ranked window, the JDBC store disables auto-commit, inserts a window header, batch-inserts the ranked items, and commits the transaction. The header and all ranks therefore become visible together. A reader cannot observe a header with only part of its Top 10.

We use one persistence worker, which preserves the FIFO order established by the queues. If a database operation raises a `StoreFailure`, that worker keeps the current window and retries it. The delays are 50 milliseconds, then 200 milliseconds, and then one second for each later attempt. It does not dequeue a newer window while the earlier one is still failing.

There is also an idempotency case to handle. The database may commit successfully while the application loses the response. On retry, the store can find an equivalent committed window and treat that retry as success rather than writing a conflicting duplicate.

This design gives readers atomic, committed snapshots, but it has a trade-off. The upstream queues are unbounded. That protects checkout latency during a short database problem, but a long outage can create a growing backlog and eventually memory pressure. I’ll return to that in the reflection slide.

## Slide 8 — End markers drain the pipeline in order

Lifecycle behavior is part of the pipeline design, not an afterthought.

The analytics service begins in `NEW`, enters `RUNNING`, and accepts scan events. When shutdown starts, it stops accepting new ingestion and moves into a draining state.

For normal shutdown, the service places an `IngressEnd` marker behind all previously accepted scan messages. Because the ingress queue is FIFO, the window worker processes every earlier scan before it sees the marker. It then forwards the corresponding end marker to the ranking stage. Ranking does the same for persistence.

The end signal therefore follows the same route as the data. Each stage drains its earlier messages before exiting, and the service waits for all worker threads within one shared shutdown deadline.

Abnormal shutdown follows a different path. If a worker fails unexpectedly, the service records the failure and interrupts the remaining workers. If the shared deadline expires, or if shutdown itself is interrupted, the service forces termination instead of waiting forever.

The key idea is that normal termination preserves order, while failure handling remains bounded.

## Slide 9 — The API returns the latest durable window

The read side stays much simpler than the write pipeline.

The client calls `GET /analytics/popular-items` with a limit. The HTTP handler validates that limit and asks `AnalyticsService` for the latest popular items. The service delegates to `PopularWindowStore.readLatest`, and the handler serializes the returned snapshot.

The sample response identifies both the window configuration and its exact bounds. In this example, the result covers accepted scans 501 through 1,500, and each item includes its SKU, scan count, and rank.

Most importantly, this request never reads the live ring buffer or a ranking that is still moving through the queues. It only reads the latest committed database snapshot. That gives the endpoint a clear consistency rule: the response may lag behind current scanning activity, but it is complete and durable.

## Slide 10 — The scan path stays stable under 10x station concurrency

We verified both the functional boundaries and the performance behavior.

The boundary tests check the exact hopping-window rules. At 999 accepted scans, there is no window. Scan 1,000 emits the first window, covering 1 through 1,000. Scan 1,499 does not emit another result. Scan 1,500 emits the next window, covering 501 through 1,500.

The load reports compare 10 stations with 100 stations. With 10 stations, scan p99 was 0.43 milliseconds. With 100 stations, it was 0.46 milliseconds. Both runs reported zero scan errors, even though the second workload increased station concurrency by a factor of ten.

The item throughput values also show that the system continued processing a substantial scan rate while analytics ran asynchronously.

There is one distinction to keep clear. Zero scan errors does not mean every transaction completed successfully. Completion can still fail for inventory reasons because it is a separate business operation. Popular-items analytics counts accepted basket scans, including scans from a basket that may later fail to complete.

These results support the architectural goal: analytics runs off the synchronous scan path without materially changing scan latency in the submitted workloads.

## Slide 11 — Pipeline fit and trade-offs

To close, the pipeline worked well because its boundaries match the natural transformations in the analytics problem.

The queues isolate checkout latency from the heavier analytics work. Immutable messages reduce concurrency risk. Deterministic ranking makes results repeatable. Transactional persistence prevents readers from seeing partial rankings.

The design also leaves clear next steps. The biggest risk is the unbounded ingress queue. A production version should either bound that queue or durably spool events so a sustained downstream outage cannot consume memory indefinitely.

We would also publish queue depth and processing lag as operational metrics. The retry policy needs an explicit budget and alerting so repeated persistence failures become visible. Finally, the ranking stage could maintain counts incrementally between overlapping windows rather than recounting all 1,000 entries each time.

The main lesson is not that pipeline architecture is the best choice for every part of this application. It is a good fit here because accepted scan data already moves through independent transformation stages, and that work does not belong on the customer’s synchronous checkout path.

That concludes the code walk. I’m happy to take questions.

## Optional short answers for likely questions

**Why count accepted scans instead of completed purchases?**  
The feature is defined around scan popularity, so the event enters analytics when the basket accepts the scan. Purchase completion is a later operation with different failure conditions, including inventory convergence.

**Why use one persistence worker?**  
One worker keeps database writes in window order and avoids concurrency between retries for adjacent windows. It favors simple ordering and recovery semantics over parallel write throughput.

**Why use overlapping windows?**  
The 500-scan slide produces a new result twice per 1,000 scans while retaining enough shared history to reduce abrupt changes between rankings.

**What happens if analytics is unavailable during a scan?**  
Checkout remains authoritative. A successfully accepted customer scan is not rolled back by an analytics failure. The failure is logged so operators can detect the gap.

**What is the largest production concern?**  
A sustained database outage can grow the unbounded queues. Queue limits or durable spooling, paired with lag metrics and alerts, would be the first production hardening step.
