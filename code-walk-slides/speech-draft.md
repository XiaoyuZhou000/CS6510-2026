# Pipeline Analytics Code Walk — Speech Draft

Estimated delivery time: 15–17 minutes, plus questions.

## Title slide — Pipeline Analytics Code Walk

Hi everyone. Today I’m going to walk through the pipeline architecture I added to the self-checkout system.

I’ll follow one simple event through the design: a customer scans an item, the checkout operation succeeds, and that accepted scan continues into an asynchronous analytics pipeline. By the end of the pipeline, the system has produced and stored a deterministic ranking of the most frequently scanned items.

My main design goal is to keep analytics from slowing down or changing the customer’s checkout result.

## Slide 1 — How I separate checkout and analytics

I first asked where a pipeline architecture actually fits.

I do not treat the complete checkout workflow as a pipeline. Starting a transaction, adding items, and completing a purchase are related business operations, and the client expects a synchronous response from each one. 

I treat the analytics path differently. I pass every accepted scan through an independent transformations. I collect scans into fixed-size windows, count and rank the SKUs in each window, and persist each completed ranking.

So I chose a narrow boundary. The public checkout API stays synchronous, while only the windowed analytics work becomes asynchronous.

## Slide 2 — I keep one checkout path and add three analytics stages

Here I show the boundary I chose.

The application retains its layered structure. The API layer handles HTTP requests, the transaction layer manages checkout logic, the analytics layer processes accepted scans, and the database access layer encapsulates persistence. Catalog and inventory operations also go through HTTP handlers, business services, and database stores. For this walkthrough, I’ll focus on the analytics pipeline and its integration with checkout.

I send an HTTP request from the load client to the API. The API delegates to the transaction service, where I validate the transaction and ask the basket to accept the scan. I keep everything up to that point on the synchronous request path.

After I accept the scan, the transaction service emits one `AcceptedScan` message. I send that message into the analytics subsystem.

From there, I run three worker stages asynchronously. I use the window filter to group accepted scans into overlapping windows with the size of 1000 scans. I use the ranking filter to convert each window into a Top 10 list. I use the persistence filter to write the completed ranking to MySQL.

I connect those stages with three blocking queues. Each stage consumes one message type, performs one transformation, and publishes a new immutable message for the next stage.

Main class connects accepted scans to the analytics pipeline. This keeps my transaction layer independent of the analytics implementation. The transaction service only knows that it can report a successfully accepted SKU.

I keep the read path separate from this graph. I send API reads to the latest ranking that I have already committed to the database, rather than to in-progress state inside the pipeline.

## Slide 3 — I create one analytics message for each accepted scan

Now let’s follow a concrete example. Suppose station 7 scans `SKU-42`.

I first ask the basket to add the scan through the transaction service. The basket checks that the transaction is still open and, if the scan is valid, updates its state and returns an accepted snapshot.

Only after that admission succeeds, I wrap the SKU in an immutable `AcceptedScan` message and offer it to the queue that buffers accepted scans before the first analytics stage.

I then return the normal synchronous scan response to the client. I do not make the client wait for the whole analytics process which includes 1,000-scan window, a ranking calculation, or a database write.

~~I care about this ordering. I never count a rejected scan because I send the event to analytics only after the basket accepts it. At the same time, I isolate analytics from checkout. If the analytics call fails, I log the failure without rolling back a scan that the basket has already accepted.~~

## Slide 4 — I give each stage one task

I use the Pipes and Filters pattern in its simplest form.

I use the window stage to transform individual accepted scans into immutable window snapshots. I use the ranking stage to turn a snapshot into an ordered Top 10. I use the persistence stage to turn that ranked message into one database commit.

I do not share mutable working data between stages. They communicate only through messages on blocking queues. This reduces the cross-thread handling I need and makes each filter easier to test in isolation.

I use FIFO queues to keep windows ordered as they move through the system. For shutdown, I place an explicit end message behind the accepted scans and send it through the same queues. This lets every stage finish earlier work before it exits.

I use `AnalyticsService` as the lifecycle owner. It creates the queues and workers, accepts new scan events, exposes the read operation, and coordinates shutdown. ~~I use `PipelineMessage` to define sealed message families so each queue carries only the messages intended for that stage.~~

## Slide 5 — I build a 1,000-scan window every 500 scans

I place the main streaming algorithm in the window filter.

I put exactly 1,000 accepted scans in each window and use a slide interval of 500, so consecutive windows overlap by half. My first result covers scans 1 through 1,000. The next covers 501 through 1,500 an so on.

I implement that with a fixed array of 1,000 slots. The `ringIndex` calculation maps each scan number to an array position. At scan 1,001, the index wraps back to slot zero and replaces the oldest value. This means the memory use for active window data stays constant even as the total number of scans grows.

I use the second function to decide when a complete window is ready. I do not emit anything before 1,000 scans. After that, I emit whenever the number of additional scans is divisible by 500. This gives me the exact boundaries shown in the table.

When a window becomes eligible, I copy the 1,000 SKUs into an immutable list. I need the copy because the ring immediately continues changing as new scans arrive. I choose to send the next stage a stable snapshot rather than a pointer of mutable storage.

## Slide 6 — I produce the Top 10 from the same scans

Ranking filter is stateless.

For this small example, my window contains A, B, A, C, A, B. I first count occurrences, which gives A three scans, B two, and C one. I then sort the entries by scan count in descending order.

Count alone is not enough because two SKUs can have the same frequency. I therefore use the SKU in ascending order as the tie-breaker. 

I didn't introduce any database dependency to filters. Given one immutable window, I produce one immutable ranking. This makes the filter step independentl from other stage in the pipeline.

## Slide 7 — I save one complete ranking at a time

For the saving stage, I use the persistence filter and one database transaction to make the result durable.

For each ranked window, the JDBC commits the top 10 ranked items to the database. 

I only use one persistence worker, which preserves the FIFO order established by the queues. If a database operation raises a `StoreFailure`, that worker keeps the current window and retries it. There will be 2 retry operations with delays of 50 milliseconds and 200 milliseconds. It will not give up a newer window if the earlier one failed.

I also handle an edge case which database may commit successfully but the client loses the response. On retry, I look for an equivalent committed window and treat that retry as success rather than writing a duplicate.

But this design has a trade-off. I currently leave the queues unbounded. This protects the enqueueing process during a short database problem, but it can introduce a memory pressure for big database.

## Slide 8 — I return the latest saved ranking

I keep the read side much simpler than the write pipeline.

When the client calls `GET /analytics/popular-items` with a limit, I validate that limit in the HTTP handler and ask `AnalyticsService` for the latest popular items. 

~~Like what i have shown In my sample, I include not only the window configuration and its exact bounds but also each item’s SKU, scan count, and rank.~~

Most importantly, I never read the live ring buffer or a ranking that is still moving through the queues. I only read the latest committed database snapshot. Although my response may lag behind current scanning activity, but it remains complete and durable.

## Slide 9 — I keep scan speed stable with 10x more stations

I verified both the functional boundaries and the performance behavior.

In my load reports, I compare 10 stations with 100 stations. With 10 stations, I measured a scan p99 of 0.43 milliseconds. With 100 stations, I measured 0.46 milliseconds. I recorded zero scan errors in both runs, even though I increased station concurrency by a factor of ten.

This report also show that I continued processing a substantial scan rate while analytics ran asynchronously.

I use boundary tests to check the exact hopping-window rules. At 999 accepted scans, I have no window. At scan 1,000, I emit the first window, covering 1 through 1,000. At scan 1,499, I do not emit another result. At scan 1,500, I emit the next window, covering 501 through 1,500.

One thing I want to clearify is that zero scan errors does not mean I completed every transaction successfully. Completion can still fail due to low inventory reasons. I treat these failures as a separate business operation in completion process of transaction service. I count accepted basket scans here for popular-items analytics.

## Slide 10 — What worked for me and what I would improve

To close, the pipeline worked well because its behavior match the analytics business logic.

I use the queues to isolate the stage, reduce concurrency risk with immutable messages. 

I also made an important performance and durability trade-off. The queues and the active ring buffer live only in process memory. Compared with writing every accepted scan to the database and reading those raw scans back to calculate each window, this design avoids database I/O for every scan. That reduces database load and helps keep the synchronous scan path fast.

The cost is crash recovery. If the process stops unexpectedly before a window is persisted, I lose the messages still in the queues and the partial window in memory. 

In a production system, I could use a durable event log so the service can replay accepted scans after a crash. Leaving queues unbounded is also a potential problem. I would either bound that queue or some triggering events to prevent outage. It would also be better to track queue depth and processing lag as operational metrics.

That concludes my code walk. I’m happy to take questions.

## Optional short answers for likely questions

**Why do I count accepted scans instead of completed purchases?**  
I define the feature around scan popularity, so I send the event to analytics when the basket accepts the scan. I treat purchase completion as a later operation with different failure conditions, including inventory convergence.

**Why do I use one persistence worker?**  
I use one worker to keep database writes in window order and avoid concurrency between retries for adjacent windows. I favor simple ordering and recovery semantics over parallel write throughput.

**Why do I use overlapping windows?**  
I use the 500-scan slide to produce a new result twice per 1,000 scans while retaining enough shared history to reduce abrupt changes between rankings.

**What do I do if analytics is unavailable during a scan?**  
I keep checkout authoritative. I do not roll back a successfully accepted customer scan because of an analytics failure. I log the failure so operators can detect the gap.

**How does the pipeline shut down without losing queued work?**  
I place an end marker behind accepted scans and send it through the same FIFO queues. Each stage finishes earlier messages before it exits. A worker failure or timeout interrupts the remaining workers instead of waiting indefinitely.

**What is my largest production concern?**  
The in-memory queues are not durable and can also grow without a bound. My first production hardening step would add durable event spooling and queue limits, paired with lag metrics and alerts.
