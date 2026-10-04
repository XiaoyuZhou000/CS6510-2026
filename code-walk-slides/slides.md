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

<p class="lede">How one accepted scan becomes a popular-items ranking</p>

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

<div class="callout">Checkout stays synchronous while analytics runs in the background.</div>

<div class="source">Pipeline Architecture Requirement.txt · README.md</div>

<!--
The assignment says the full checkout application is not a natural pipeline. Windowed analytics is different because the data already passes through independent transformations. This creates a narrow architectural boundary without changing the API contract.
Target time: 1 minute.
-->

---
layout: default
---

<div class="eyebrow">02 · Architecture</div>

# One checkout path, three analytics stages

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

<div class="callout">
  <div><strong>API:</strong> HTTP requests · <strong>Transaction:</strong> checkout logic</div>
  <div><strong>Analytics:</strong> scan processing · <strong>Database access:</strong> persistence</div>
</div>

<div class="source">server/src/api/Main.java · server/src/analytics/AnalyticsService.java</div>

<!--
Briefly introduce the four layers: API handles HTTP requests, transaction manages checkout logic, analytics processes accepted scans, and database access encapsulates persistence. Catalog and inventory also use the API, business services, and database stores. Then focus on the analytics pipeline and its integration with checkout. Main creates AnalyticsService and passes its ingestion method into TransactionService through the AcceptedScanSink interface. Three BlockingQueues connect three dedicated worker threads. Reads go to the latest committed database snapshot.
Target time: 2 minutes.
-->

---
layout: default
---

<div class="eyebrow">03 · Core flow</div>

# Each accepted scan creates one analytics message

<div class="sequence-list">
  <div class="sequence-step"><span class="sequence-actor">Client</span><span class="sequence-action">scan(transactionId, SKU-42)</span></div>
  <div class="sequence-step"><span class="sequence-actor">TransactionService</span><span class="sequence-action">Add the scan if the basket is open</span></div>
  <div class="sequence-step"><span class="sequence-actor">Basket</span><span class="sequence-action">Confirm the accepted scan</span></div>
  <div class="sequence-step"><span class="sequence-actor">AnalyticsService</span><span class="sequence-action">recordAcceptedScan(SKU-42)</span></div>
  <div class="sequence-step"><span class="sequence-actor">IngressQueue</span><span class="sequence-action">Queue one AcceptedScan message</span></div>
  <div class="sequence-step"><span class="sequence-actor">Client</span><span class="sequence-action">Receive the scan response</span></div>
</div>

<div class="callout">The scan is accepted first. An analytics failure does not undo it.</div>

<div class="source">server/src/transaction/TransactionService.java</div>

<!--
Tell the concrete story: Station 7 scans SKU-42. The basket validates the transaction and adds the item. Only then does the service emit the accepted scan. The analytics call is guarded so its failure is logged but does not change the checkout result. This is deliberate isolation, not lost error handling.
Target time: 1 minute 30 seconds.
-->

---
layout: default
---

<div class="eyebrow">04 · Pipes and filters</div>

# Three stages, one task each

<div class="stage-line">
  <div class="stage">
    <h2>Window</h2>
    <p class="muted">Accepted scans<br>to 1,000-scan windows</p>
  </div>
  <div class="pipe">queue</div>
  <div class="stage">
    <h2>Ranking</h2>
    <p class="muted">Scan windows<br>to Top 10 items</p>
  </div>
  <div class="pipe">queue</div>
  <div class="stage">
    <h2>Persistence</h2>
    <p class="muted">Top 10 rankings<br>to the database</p>
  </div>
</div>

<div class="mt-12 grid grid-cols-3 gap-8">
  <div><strong>Independent</strong><br><span class="muted">Each stage handles one task</span></div>
  <div><strong>In order</strong><br><span class="muted">Queues keep windows in sequence</span></div>
  <div><strong>Clean shutdown</strong><br><span class="muted">Every queued item finishes first</span></div>
</div>

<div class="source">server/src/analytics/AnalyticsService.java · PipelineMessage.java</div>

<!--
Name the pattern explicitly: Pipes and Filters implemented with Java BlockingQueues. AnalyticsService is the facade and lifecycle owner. For shutdown, an end marker enters behind accepted scans and follows the same queues, so each stage finishes earlier work before it exits.
Target time: 1 minute.
-->

---
layout: two-cols
layoutClass: gap-12
---

<div class="eyebrow">05 · Window filter</div>

# A 1,000-scan window updates every 500 scans

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

<div class="callout">Windows overlap by 500 scans and use only 1,000 storage slots.</div>

::right::

```java
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
This is the main algorithmic slide. At scan 1001 the ring wraps to slot zero, replacing the oldest value. A snapshot is emitted only after a full 1,000 scans, then every 500 scans. Walk through the code from the constants to the emission condition. The snapshot is copied into an immutable list before crossing the queue boundary.
Target time: 2 minutes.
-->

---
layout: two-cols
layoutClass: gap-14
---

<div class="eyebrow">06 · Ranking filter</div>

# Same scans, same Top 10

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

<div class="callout">When counts tie, the SKU name decides the order.</div>

::right::

```java
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

# Each save contains one complete ranking

<div class="flow-row mt-10">
  <div class="flow-node hot">New ranking</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Create window</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Add Top 10</div><div class="flow-arrow">-&gt;</div>
  <div class="flow-node hot">Save</div>
</div>

<div class="grid grid-cols-2 gap-14 mt-8">
<div>

## How writes work

- Windows remain in order
- The window and its items are written together
- Retries do not create duplicates
- Readers see only completed rankings

</div>
<div>

<div class="callout warning"><strong>Database error:</strong> the same ranking retries after a delay.</div>

<div class="callout warning">A long database outage can build up queued work.</div>

</div>
</div>

<div class="source">server/src/analytics/PersistenceFilter.java · database/JdbcPopularWindowStore.java</div>

<!--
The persistence filter converts the pipeline message into a store snapshot. JDBC turns off auto-commit, inserts one header and all ranked items, then commits. If the connection fails after the commit response is lost, the store recognizes an equivalent existing window and treats the retry as success.
Target time: 1 minute 45 seconds.
-->

---
layout: two-cols
layoutClass: gap-14
---

<div class="eyebrow">08 · Read path</div>

# The API returns the latest saved ranking

```text
GET /analytics/popular-items?limit=10
              │
              ▼
AnalyticsHttpHandler
              │
              ▼
AnalyticsService.latestPopularItems()
```

<div class="callout">The API returns the latest committed database snapshot.</div>

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
The read path is deliberately simple. The handler validates the limit, and AnalyticsService returns the latest committed database snapshot. The sample response shows the window configuration, exact bounds, SKU, scan count, and rank.
Target time: 1 minute.
-->

---
layout: default
---

<div class="eyebrow">09 · Verification</div>

# Scan speed stays stable with 10x more stations

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

<div class="callout">Popular-items counts accepted scans. Purchase completion is measured separately.</div>

<div class="source">load-client/reports/pipeline-architecture/*.json · server/tests/unit/RingBufferWindowMathTest.java</div>

<!--
Use the real submitted reports. Increasing stations from 10 to 100 raises scan p99 only from 0.43 to 0.46 milliseconds, with zero scan errors in both runs. Do not claim the entire system has zero errors: completion errors exist because completion is a separate business operation. The boundary tests verify the exact hopping-window math.
Target time: 1 minute 15 seconds.
-->

---
layout: default
---

<div class="eyebrow">10 · Reflection</div>

# What worked and what to improve

<div class="compare-grid mt-8">
<section>

## What worked

- Stages match the analytics workflow
- In-memory queues avoid per-scan database I/O
- Immutable messages reduce concurrency risk

</section>
<section>

## Next improvements

- Replay scans after a process failure
- Limit queue growth during an outage
- Monitor queue depth and processing lag

</section>
</div>

<blockquote>In-memory queues avoid per-scan database I/O, but a process crash loses queued scans.</blockquote>

<!--
The pipeline stages match the analytics workflow, and immutable messages reduce shared-state risk. Keeping scans in memory avoids writing every scan to the database and reading raw scans back for each calculation. The trade-off is durability: a crash loses queued scans and the partial active window. A durable event log would allow replay after a crash. The unbounded queue also protects request latency at the cost of memory pressure during a sustained downstream outage, so queue depth and processing lag should be monitored.
Target time: 2 minutes, then invite questions.
-->
