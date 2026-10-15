# From Pipeline Architecture to Distributed Services: Stack, Structure, and Optimization

**Project:** Self-Checkout System  
**Period:** Pipeline baseline (October 1, 2026) through transaction tuning (October 14, 2026, Pacific)  
**Status:** The service split, SQL Materializer, and fixed 32-connection Scan tuning are committed. The growing 32-to-64-connection pool and its transaction optimization evidence are in the current, uncommitted working tree.

## 1. What changed after Pipeline Architecture

The Pipeline Architecture ran one Java 21 server process. Its analytics path admitted accepted Scans into an in-process, three-filter pipeline: **Window → Ranking → Persistence**, connected by Java `BlockingQueue` pipes. Checkout and the pipeline shared a JVM; a Scan was acknowledged before the analytics event became durable. MySQL already stored checkout and completed popularity windows, and the project already used plain `javac`/`jar`, JDBC, Connector/J, JUnit, and the JDK HTTP server. These technologies were retained, not newly introduced by the service split. [Pipeline design](specs/003-pipeline-analytics/plan.md) · [Pipeline measurements](load-client/reports/pipeline-architecture/README.md)

The new deployment separates business domains into **three independently buildable and restartable Java 21 JVMs**. Nginx is the new public router and keeps the same seven-route API at `localhost:8080`. All three services use one MySQL 8.4/InnoDB database; they do not call one another over HTTP. A small `common.jar` contains shared HTTP/JSON and JDBC utilities, while each domain JAR owns its handlers, business logic, and database adapters. The build remains script based, without Maven, Gradle, or an application framework. [Service guide](server/README.md) · [Deployment contract](specs/004-distributed-domain-services/contracts/service-deployment.md)

| Component | Structure and responsibility | Public routes |
|---|---|---|
| Nginx, `deploy/nginx.conf` | Routes the unchanged public API on port 8080; reuses upstream connections. | All seven routes |
| Transaction, `server/transaction/`, port 8081 | Starts and completes checkouts; commits each accepted Scan's basket change and raw event together; conditionally decrements stock at completion. | `/transactions` and descendants |
| Inventory, `server/inventory/`, port 8082 | Owns catalog and low-stock reads. | `/items`, `/inventory/low-stock` |
| Analytics, `server/analytics/`, port 8083 | Materializes raw Scan events into positions, runs the Window/Ranking/Persistence pipeline, and serves the latest committed popularity window. | `/analytics/popular-items` |
| Shared code, `server/common/` | HTTP/JSON support, JDBC connection pooling, and shared persistence utilities. | None |
| MySQL, `db/` | Shared durable basket, stock, raw-event, position, and analytics-checkpoint state. | None |

The storage contract evolved twice. The first service implementation made every successful Scan durable and allocated its analytics position under a singleton `scan_sequence` row in the Scan transaction. That kept event positions gapless, but serialized Scan writers. The SQL Materializer redesign gave each raw event an immutable `event_id` and a nullable `position`. Transaction now commits the basket and raw event without touching `scan_sequence`; Analytics assigns contiguous positions in bounded batches and commits those assignments with the checkpoint. Its poller then feeds only committed positioned events through the existing three filters. This also lets Analytics recover after an outage and replay from its last committed window. [Materializer design](specs/004-distributed-domain-services/sql-materializer-design.md) · [Schema](db/init.sql) · [Migration](db/migrate-sql-materializer.sql)

```text
Unchanged client → Nginx :8080 ─┬→ Transaction :8081 ─┐
                                 ├→ Inventory :8082 ───┼→ shared MySQL
                                 └→ Analytics :8083 ───┘
                                      raw-event poll → position batches
                                      → Window → Ranking → Persistence
```

## 2. Optimization steps and measured results

The same unmodified load client used **10 stations for 60 seconds** (default) and **100 stations for 120 seconds** (stress), with 1–20 Scans per basket. Each retained service-era workload used a fresh database reset. `Scans/s` counts client-acknowledged successful Scans; `completed/s` counts baskets whose successful Complete response was received and parsed. The table uses one representative retained run per stage, so it is a progression of measurements rather than a statistical estimate of every configuration's mean. [Client metric definition](load-client/reports/transaction-optimization/README.md)

| Stage | Default: Scans/s; Scan p95/p99 | Stress: Scans/s; Scan p95/p99 | Stress completed/s | Result |
|---|---:|---:|---:|---|
| Pipeline baseline, in-process admission | 2,144.35; 0.24/0.43 ms | 3,698.38; 0.26/0.46 ms | 159.42 | Fast admission, but accepted events were not durably committed on Scan. [Raw default](load-client/reports/pipeline-architecture/report-20261001-194225.json) · [raw stress](load-client/reports/pipeline-architecture/report-20261001-194550.json) |
| First distributed service implementation, commit-time position | 172.76; 74.10/140.07 ms | 177.23; 1,446.49/2,836.81 ms | 17.14 | Durable Scan and service separation exposed a severe Scan bottleneck; stress had seven client timeouts. [Evidence](load-client/reports/service-based-architecture/README.md) |
| SQL Materializer, Transaction pool 10 | 855.88; 13.82/32.32 ms | 837.73; 215.67/330.24 ms | 70.44 | Moving sequence assignment into Analytics raised Scan throughput **4.95× default** and **4.73× stress** against the first service run. No Scan errors; stress Complete had 1,169 expected stock rejections. [Evidence](load-client/reports/sql-materializer/README.md) |
| Fixed Transaction pool 32, retained Scan tuning | 815.17; 14.58/34.60 ms | 1,816.63; 91.74/153.53 ms | 100.42 | Stress Scan throughput reached **2.15×** the fresh pool-10 control (844.20/s); three pool-32 stress trials gave 1,729.94–1,816.63/s. No demonstrated default-workload gain. [Evidence](load-client/reports/scan-optimization/README.md) |
| Growing Transaction pool 32→64, current working tree | 838.68; 14.22/36.79 ms | 2,320.74; 61.96/106.41 ms | 117.80 | A representative stress run completed **14,164 baskets** versus **12,282** in its fresh fixed-32 control (+15.3%). Three growing-pool runs completed 13,897/14,304/14,164 baskets. Default medians were 4,814 before and 4,751 after, so no default gain was established. [Evidence](load-client/reports/transaction-optimization/README.md) |

### Step 1 — Durable domain-service split

The split preserved the synchronous REST contract and unchanged client while moving Transaction, Inventory, and Analytics into separate processes. An accepted Scan now committed the basket update and an event before HTTP 200; Analytics could replay durable events after restart. Initially, Scan also acquired the `scan_sequence` lock to allocate a gapless position. Against the Pipeline baseline, the first distributed stress run fell from **3,698.38 to 177.23 Scans/s**, while Scan p95 rose from **0.26 to 1,446.49 ms**. This comparison includes the cost of new durability and routing, so it does not isolate the sequence lock alone. The database recorded 21,927 committed accepted Scans against 21,926 acknowledged Scans in stress: one timed-out response followed a committed Scan. SQL invariants passed. [Service evidence](load-client/reports/service-based-architecture/README.md)

### Step 2 — Move sequence assignment to the SQL Materializer

Transaction stopped locking the singleton sequence row. It writes a raw event with a unique ID in the same commit as its basket update; Analytics assigns positions later in batches and atomically advances `scan_sequence.last_position`. Default throughput rose **172.76 → 855.88 Scans/s**, and stress rose **177.23 → 837.73 Scans/s**. Scan p95 fell **74.10 → 13.82 ms** default and **1,446.49 → 215.67 ms** stress. The Materializer processed about **748 events/s default** and **780 events/s stress** over sampled intervals, with sampled pending peaks of 18 and 70 events and zero pending after drain. Both full invariant checks passed, including positions, every complete 1,000/500 analytics window, ranks, basket totals, and stock. [Materializer evidence](load-client/reports/sql-materializer/README.md)

### Step 3 — Profile Scan and raise its connection-pool limit

`SCAN_PROFILE=true` exposed the Scan phases without logging request contents. At 100 stations, a fixed pool of 10 had about **88 waiters** and **87.65 ms mean acquisition wait**; its Scan commit itself averaged about **8.13 ms**. Raising only Transaction's pool to 32 reduced mean acquisition wait to **29.68 ms** and gave **1,761.50**, **1,729.94**, and **1,816.63 Scans/s** in three stress trials, versus **844.20/s** for the fresh pool-10 control. Retained stress Scan p95/p99 fell **212.46/319.28 → 91.74/153.53 ms**. At 10 stations there were no pool waiters, and the change gave no reliable improvement. Inventory and Analytics stayed at 10 connections. All retained benchmark invariants passed, and the full live suite reported **142 successful tests, zero failures**. [Scan evidence](load-client/reports/scan-optimization/README.md)

### Step 4 — Profile the full transaction lifecycle and grow the pool on demand

The next profile covered Start, Scan, Complete, pool wait, commits, and handler time. With fixed 32 under stress, Start/Scan/Complete pool waits averaged **30.39/29.03/28.71 ms**. Each basket averaged about **10.5 Scans**, making repeated Scan wait a larger limit on completed baskets than a single Complete call. A pool starting at 32 and growing to at most 64 reduced these waits to **13.60/11.22/11.21 ms** in the representative stress comparison. Successful Scans rose **223,171 → 279,030**, and client-counted completed baskets rose **12,282 → 14,164**. Scan p95/p99 improved **91/152 → 62/106 ms**. [Transaction evidence](load-client/reports/transaction-optimization/README.md)

The tradeoff shifted to checkout contention: successful Complete p95/p99 rose **331/392 → 727/867 ms**, and expected `409 INSUFFICIENT_STOCK` responses rose **8,933 → 12,419** as more baskets reached scarce popular items. The sampled peak pending Materializer work in the representative growing-pool run was **883 events / 831 ms oldest age**, then drained to zero. Across all three retained stress runs, the oldest sampled pending age was at most 831 ms. All valid-run SQL invariants passed; the full live suite reported **143 successful tests, zero failures**, plus the intentionally disabled destructive reset test. These pool changes and the evidence directory are still uncommitted. [Transaction evidence](load-client/reports/transaction-optimization/README.md)

### Other measured candidates

| Experiment | Measurement | Decision |
|---|---|---|
| Join captured line price into the catalog read | 837.44 default and 1,796.52 stress Scans/s; default repeat 825.38 with worse tails. | Reverted; price-capture regression test retained. [Scan evidence](load-client/reports/scan-optimization/README.md) |
| Delay small Materializer batches by 5 ms | Stress trials 1,837.30, 1,804.50, then 1,704.75 Scans/s; sampled backlog reached 1,258 events / 669 ms. | Reverted because the gain was inconsistent and freshness worsened. [Scan evidence](load-client/reports/scan-optimization/README.md) |
| Drop `idx_scan_pending` | 843.46 default and 1,737.29 stress Scans/s; `EXPLAIN` was already using `uq_scan_position`. | Reverted; no clear gain. Combined pool/index trials were excluded from the retained pool A/B comparison. [Scan evidence](load-client/reports/scan-optimization/README.md) |
| Fixed Transaction pool 64 or 96 | Pool 64 completed 14,443/14,552/14,570 stress baskets but only 4,304/4,704 in default trials. Pool 96 completed 15,385/15,784 stress baskets, but pending backlog reached 14,577/24,617 events and oldest age 3.4/5.6 s. | Retained on-demand 32→64 to protect default behavior and limit lag. [Transaction evidence](load-client/reports/transaction-optimization/README.md) |
| Insufficient-stock precheck; reverse inventory lock order | Fixed-32 precheck: 12,162 stress baskets; fixed-64 precheck: 14,876/14,407/14,567. Reverse lock order at pool 96: 15,713, but Complete p99 rose to 1.82 s. | Reverted: no reliable overall improvement or unacceptable Complete tail. [Transaction evidence](load-client/reports/transaction-optimization/README.md) |

## Interpretation and limits

The latest representative distributed run is **13.1×** the first distributed stress Scan rate (**2,320.74 versus 177.23/s**), with durable raw events and a recoverable Analytics pipeline. It is still below the earlier in-process Pipeline baseline of **3,698.38 Scans/s**; the Pipeline acknowledged Scans without the new per-Scan durable write. The final default run similarly reached **838.68 Scans/s**, below Pipeline's **2,144.35/s**. These are different durability contracts, and the pipeline-to-service comparison should not be presented as an isolated architecture overhead measurement.

The benchmark results are host-specific. Materializer and pool trials used fresh, isolated resets of `cs6510_materializer_bench`, the same unmodified client workloads, and MySQL durability settings `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`. One representative row is shown for each stage; the linked experiment records contain repeats and raw JSON. Complete throughput and error counts are influenced by the Zipf-heavy item distribution and stock depletion, so Scan throughput, Complete count, tail latency, pending-event lag, and invariant checks need to be read together. [Benchmark methodology](load-client/reports/transaction-optimization/README.md) · [Invariant SQL](db/validate-invariants.sql)
