---
theme: default
title: Pipeline Analytics Code Walk
info: CS6510 self-checkout pipeline architecture walkthrough
author: CS6510
aspectRatio: 16/9
canvasWidth: 1280
colorSchema: dark
highlighter: shiki
lineNumbers: true
transition: slide-left
download: true
exportFilename: pipeline-code-walk
---

<div class="eyebrow">CS6510 · Code Walk</div>

# Pipeline Analytics
# Code Walk

<div class="accent-line"></div>

<p class="lede">How one accepted scan becomes a durable, deterministic popular-items ranking</p>

<div class="mt-14 muted">Self-Checkout System · Pipeline Architecture</div>

<!--
Open with the scenario: a customer scans one item. The checkout response remains synchronous, but analytics continues asynchronously. The talk follows that one scan through the complete pipeline.
Target time: 30 seconds.
-->

---
layout: default
---

<div class="eyebrow">01 · Scope</div>

# Pipeline where the data already flows

<div class="compare-grid mt-8">
<section>

## Checkout path

- Start a transaction
- Admit a basket scan
- Complete the purchase atomically
- Return a synchronous response

</section>
<section>

## Analytics path

- Observe accepted scans
- Form fixed-size windows
- Rank the most scanned SKUs
- Persist completed rankings

</section>
</div>

<div class="callout">The public API remains synchronous. Only windowed analytics becomes a pipeline.</div>

<div class="source">Pipeline Architecture Requirement.txt · README.md</div>

<!--
The assignment says the full checkout application is not a natural pipeline. Windowed analytics is different because the data already passes through independent transformations. That gives us a narrow architectural boundary without changing the API contract.
Target time: 1 minute.
-->

---
layout: default
---

<div class="eyebrow">02 · Architecture</div>

# One synchronous boundary, three asynchronous stages

<div class="flow-row">
  <div class="flow-node">Load Client</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node">HTTP API</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node">Transaction Service</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">AcceptedScan</div>
</div>

<div class="flow-row">
  <div class="flow-node hot">Window Filter</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Ranking Filter</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node warn">Persistence Filter</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node">MySQL</div>
</div>

<div class="pipe-labels"><span>ingressQueue</span><span>windowQueue</span><span>rankingQueue</span></div>

<div class="callout">`Main` composes the graph and injects `analytics::recordAcceptedScan` into the transaction service.</div>

<div class="source">server/src/api/Main.java · server/src/analytics/AnalyticsService.java</div>

<!--
Point out the architectural seam. Main creates AnalyticsService and passes its ingestion method into TransactionService through the AcceptedScanSink interface. Three BlockingQueues connect three dedicated worker threads. Reads go to the latest committed database snapshot.
Target time: 2 minutes.
-->

---
layout: default
---

<div class="eyebrow">03 · Core flow</div>

# A successful basket scan emits one analytics message

<div class="sequence-list">
  <div class="sequence-step"><span class="sequence-actor">Client</span><span class="sequence-action">scan(transactionId, SKU-42)</span></div>
  <div class="sequence-step"><span class="sequence-actor">TransactionService</span><span class="sequence-action">Ask the basket to add the scan if it is still open</span></div>
  <div class="sequence-step"><span class="sequence-actor">Basket</span><span class="sequence-action">Return an accepted snapshot</span></div>
  <div class="sequence-step"><span class="sequence-actor">AnalyticsService</span><span class="sequence-action">recordAcceptedScan(SKU-42)</span></div>
  <div class="sequence-step"><span class="sequence-actor">IngressQueue</span><span class="sequence-action">Offer one immutable AcceptedScan message</span></div>
  <div class="sequence-step"><span class="sequence-actor">Client</span><span class="sequence-action">Receive the synchronous scan response</span></div>
</div>

<div class="callout">Admission happens first. An analytics outage cannot roll back an accepted customer scan.</div>

<div class="source">server/src/transaction/TransactionService.java</div>

<!--
Tell the concrete story: Station 7 scans SKU-42. The basket validates the transaction and adds the item. Only then do we emit the accepted scan. The analytics call is guarded so its failure is logged but does not change the checkout result. This is deliberate isolation, not lost error handling.
Target time: 1 minute 30 seconds.
-->

---
layout: default
---

<div class="eyebrow">04 · Pipes and filters</div>

# Each filter performs one transformation

<div class="stage-line">
  <div class="stage">
    <h2>Window</h2>
    <p class="muted">Accepted scans<br>to immutable windows</p>
  </div>
  <div class="pipe">queue</div>
  <div class="stage">
    <h2>Ranking</h2>
    <p class="muted">Window snapshots<br>to deterministic Top 10</p>
  </div>
  <div class="pipe">queue</div>
  <div class="stage">
    <h2>Persistence</h2>
    <p class="muted">Ranked windows<br>to atomic DB commits</p>
  </div>
</div>

<div class="mt-12 grid grid-cols-3 gap-8">
  <div><strong>Isolation</strong><br><span class="muted">Stages share messages, not mutable state</span></div>
  <div><strong>Ordering</strong><br><span class="muted">FIFO queues preserve window order</span></div>
  <div><strong>Lifecycle</strong><br><span class="muted">End markers drain every stage</span></div>
</div>

<div class="source">server/src/analytics/AnalyticsService.java · PipelineMessage.java</div>

<!--
Name the pattern explicitly: Pipes and Filters implemented with Java BlockingQueues. AnalyticsService is the facade and lifecycle owner. PipelineMessage uses sealed message families so each queue accepts only the message types intended for that stage.
Target time: 1 minute.
-->

---
layout: two-cols
layoutClass: gap-12
---

<div class="eyebrow">05 · Window filter</div>

# A ring buffer forms exact 1,000-scan windows

<div class="window-strip">
  <span>1-500</span>
  <span>501-1000</span>
  <span>1001-1500</span>
  <span>1501-2000</span>
</div>

| Emission | Window bounds |
|---:|:---|
| Scan 1000 | 1-1000 |
| Scan 1500 | 501-1500 |
| Scan 2000 | 1001-2000 |

<div class="callout">The window overlaps by 500 scans, but storage remains fixed at 1,000 array slots.</div>

::right::

```java {1-2|4-6|8-13|all}
static final int WINDOW_SIZE = 1_000;
static final int SLIDE_INTERVAL = 500;

static int ringIndex(long scanNumber) {
    return (int) ((scanNumber - 1) % WINDOW_SIZE);
}

static boolean isEmissionEligible(long accepted) {
    return accepted >= WINDOW_SIZE
        && (accepted - WINDOW_SIZE)
            % SLIDE_INTERVAL == 0;
}
```

<div class="source">server/src/analytics/WindowMath.java · WindowFilter.java</div>

<!--
This is the main algorithmic slide. At scan 1001 the ring wraps to slot zero, replacing the oldest value. A snapshot is emitted only after a full 1,000 scans, then every 500 scans. Walk through the three highlighted code steps. The snapshot is copied into an immutable list before crossing the queue boundary.
Target time: 2 minutes.
-->

---
layout: two-cols
layoutClass: gap-14
---

<div class="eyebrow">06 · Ranking filter</div>

# Same window, same Top 10

## Example window

```text
A, B, A, C, A, B
```

## Ranked output

```text
1  A   3 scans
2  B   2 scans
3  C   1 scan
```

<div class="callout">SKU ascending breaks count ties, so results stay reproducible across runs.</div>

::right::

```java {1-2|4-6|8-13|all}
Map<String, Long> counts = new HashMap<>();
for (String sku : snapshot.skus())
    counts.merge(sku, 1L, Long::sum);

List<Map.Entry<String, Long>> ordered =
    new ArrayList<>(counts.entrySet());

ordered.sort(
    Map.Entry.<String, Long>
        comparingByValue(Comparator.reverseOrder())
        .thenComparing(Map.Entry.comparingByKey()));
```

<div class="source">server/src/analytics/RankingFilter.java</div>

<!--
The ranking stage has no database dependency and no mutable state between messages. First it counts each SKU. Then it sorts by descending count and ascending SKU. That second rule matters because deterministic ties make tests and architecture comparisons reliable.
Target time: 1 minute 15 seconds.
-->

---
layout: default
---

<div class="eyebrow">07 · Persistence filter</div>

# A ranking becomes visible only after one atomic commit

<div class="flow-row mt-10">
  <div class="flow-node hot">Ranked window</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Insert header</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Batch insert ranks</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Commit</div>
</div>

<div class="grid grid-cols-2 gap-14 mt-8">
<div>

## Reliability decisions

- One worker preserves FIFO ordering
- Header and ranks share one transaction
- Equivalent committed windows make retry safe
- Readers only observe committed snapshots

</div>
<div>

<div class="callout warning"><strong>StoreFailure:</strong> retry after 50 ms, 200 ms, then 1 second per attempt.</div>

<div class="callout warning">A long database outage can grow the upstream queue.</div>

</div>
</div>

<div class="source">server/src/analytics/PersistenceFilter.java · database/JdbcPopularWindowStore.java</div>

<!--
The persistence filter converts the pipeline message into a store snapshot. JDBC turns off auto-commit, inserts one header and all ranked items, then commits. If the connection fails after the commit response is lost, the store recognizes an equivalent existing window and treats the retry as success.
Target time: 1 minute 45 seconds.
-->

---
layout: default
---

<div class="eyebrow">08 · Failure and lifecycle</div>

# End markers drain the pipeline in order

<div class="state-track">
  <div class="state-node">NEW</div><div class="flow-arrow">-&gt;</div>
  <div class="state-node">RUNNING</div><div class="flow-arrow">shutdown -&gt;</div>
  <div class="state-node">DRAINING</div>
</div>

<div class="state-branch">
  <div class="state-node"><strong>TERMINATED</strong><br><span class="muted">all workers exit</span></div>
  <div class="state-node"><strong>FORCED</strong><br><span class="muted">timeout, interruption, or failure</span></div>
</div>

<div class="grid grid-cols-2 gap-14 mt-6">
<div>

## Normal shutdown

`IngressEnd` passes through Window, Ranking, and Persistence after earlier messages.

</div>
<div>

## Abnormal shutdown

A worker failure marks the service failed; timeout or interruption forces worker termination.

</div>
</div>

<div class="callout">Shutdown preserves order because the end marker travels through the same FIFO queues as the data.</div>

<div class="source">server/src/analytics/AnalyticsService.java · PipelineMessage.java</div>

<!--
Once shutdown starts, ingestion is rejected. The end marker is enqueued behind accepted scans, so workers finish earlier messages before exiting. AnalyticsService joins each thread within a shared timeout. Any worker failure transitions the service to FAILED and interrupts the remaining workers.
Target time: 1 minute 30 seconds.
-->

---
layout: two-cols
layoutClass: gap-14
---

<div class="eyebrow">09 · Read path</div>

# The API returns the latest durable window

```text
GET /analytics/popular-items?limit=10
              │
              ▼
AnalyticsHttpHandler
              │
              ▼
AnalyticsService.latestPopularItems()
              │
              ▼
PopularWindowStore.readLatest()
```

<div class="callout">The HTTP request never reads the live ring buffer.</div>

::right::

```json
{
  "windowSize": 1000,
  "slideInterval": 500,
  "windowStart": 501,
  "windowEnd": 1500,
  "items": [
    {
      "sku": "SKU-000001",
      "scanCount": 117,
      "rank": 1
    }
  ]
}
```

<div class="source">server/src/api/AnalyticsHttpHandler.java · analytics/AnalyticsService.java</div>

<!--
The read path is deliberately simple. The handler validates limit, the service asks the store for the latest committed snapshot, and the handler serializes it. The sample boundaries illustrate the second emitted window; the rank values come from the default run report.
Target time: 1 minute.
-->

---
layout: default
---

<div class="eyebrow">10 · Verification</div>

# The scan path stays stable under 10x station concurrency

| Workload | Transactions/s | Items/s | Scan p99 | Scan errors |
|:---|---:|---:|---:|---:|
| 10 stations · 60 s | 153.5 | 2,144.3 | 0.43 ms | 0% |
| 100 stations · 120 s | 159.4 | 3,698.4 | 0.46 ms | 0% |

<div class="metric-row">
  <div><div class="number">999</div><div class="number-label">no window yet</div></div>
  <div><div class="number">1000</div><div class="number-label">emit 1-1000</div></div>
  <div><div class="number">1499</div><div class="number-label">no new window</div></div>
  <div><div class="number">1500</div><div class="number-label">emit 501-1500</div></div>
</div>

<div class="callout">Completion errors are a separate inventory outcome. Popular-items counts accepted scans, including baskets that may not later complete.</div>

<div class="source">load-client/reports/pipeline-architecture/*.json · server/tests/unit/RingBufferWindowMathTest.java</div>

<!--
Use the real submitted reports. Increasing stations from 10 to 100 raises scan p99 only from 0.43 to 0.46 milliseconds, with zero scan errors in both runs. Do not claim the entire system has zero errors: completion errors exist because completion is a separate business operation. The boundary tests verify the exact hopping-window math.
Target time: 1 minute 15 seconds.
-->

---
layout: default
---

<div class="eyebrow">11 · Reflection</div>

# Pipeline fit and trade-offs

<div class="compare-grid mt-8">
<section>

## What worked

- Stage boundaries match the data transformations
- Queues isolate checkout latency from analytics work
- Immutable messages simplify concurrency
- Deterministic ranking supports repeatable tests
- Transactional storage prevents partial results

</section>
<section>

## What comes next

- Bound or durably spool the ingress queue
- Publish queue depth and processing-lag metrics
- Add an explicit retry budget and alerting
- Maintain counts incrementally between windows
- Exercise recovery during a sustained DB outage

</section>
</div>

<blockquote>Pipeline architecture works best where data naturally crosses independent transformation stages.</blockquote>

<!--
The main benefit is not that pipelines are universally better. It is that this analytics feature already has clear transformation stages and does not belong on the checkout response path. The key trade-off is the unbounded queue: it protects request latency, but a sustained downstream outage can turn backlog into memory pressure.
Target time: 2 minutes, then invite questions.
-->
