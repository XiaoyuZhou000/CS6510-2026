# Transaction lifecycle optimization — October 14, 2026 (Pacific)

## What the unchanged client counts

`totalTransactions` is the sum of `StationWorker.transactionsCompleted` over all stations. A worker increments that counter only after its Start request, all randomly selected Scans, and its Complete request have returned, the Complete response has status 200 and a parseable receipt, and `metrics.recordSuccess(COMPLETE_TRANSACTION, …)` has returned. A failed Start, Scan, Complete, transport call, or response parse does not increment it. Each worker checks the stop flag before starting the next basket; after the requested 60 or 120 seconds it finishes its current basket, so a final success after the timer can be included. The JSON writer copies this sum to `totalTransactions`. `transactionsPerSecond` divides it by observed wall-clock time, including in-flight drain; that rate is secondary. In these valid runs `totalTransactions` equals the client’s `COMPLETE_TRANSACTION.successCount`, but neither field is a count of server-side durable commits whose response never reached the client. Source: `load-client/src/StationWorker.java`, `Main.java`, `ApiClient.java`, and `ReportWriter.java`.

## Controlled setup and primary result

The unmodified client used the standard 10-station/60-second and 100-station/120-second workloads, with 1–20 random Scans per basket, through Nginx and separate Java 21 Transaction, Inventory, and Analytics services. Every listed run reset **only** `cs6510_materializer_bench` from `db/init.sql`; each began with 2,000 products and 10,000 units per product. MySQL 8.4 retained `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`. Inventory and Analytics used fixed 10-connection pools. The baseline Transaction pool was fixed at 32; the retained pool starts at 32 and grows to at most 64 only under demand. The client source, REST routes and JSON, database schema, inventory update rules, and Materializer checkpoint logic were unchanged.

| Workload | Fixed 32 before: `totalTransactions` | Growing 32→64 after: `totalTransactions` | Median change |
|---|---:|---:|---:|
| 10 stations, 60 s | 4,867 / 4,814 / 4,785 | 4,750 / 4,751 / 4,804 | 4,814 → 4,751 (−1.3%; no demonstrated default gain) |
| 100 stations, 120 s | 12,282 in this series; prior equivalent fixed-32 runs: 11,912 / 11,717 / 12,087 | 13,897 / 14,304 / 14,164 | 12,282 → 14,164 (+15.3% against the fresh control); 11,999.5 → 14,164 (+18.0% against all fixed-32 controls) |

The stress result improves in all three fresh repeats; its lowest after count exceeds the highest fixed-32 control by 1,615. At 10 stations the retained pool never exceeded 32 connections, and no pool wait was measured. The small default count difference is within the scale of observed run variation and is not a demonstrated improvement. The chosen configuration trades longer successful Complete latency for more client-counted transactions under stress; retained stress p95/p99 remained below 0.75/0.87 seconds, with no timeouts, and sampled Materializer lag stayed below 0.84 seconds. Those measured tails were accepted in favor of the primary count; the much larger lag at pool 96 was not.

Representative raw client JSON: [before default](pool32-default-2/default/report-20261014-170742.json), [before stress](pool32-stress-2/stress/report-20261014-170958.json), [after default](grow32to64-default-3/default/report-20261014-175554.json), [after stress](grow32to64-stress-3/stress/report-20261014-175114.json). Every run directory also includes two-second MySQL samples, aggregate phase logs, client error lines, and SQL invariant output. The previous fixed-32 repeat reports are in [the Scan optimization record](../scan-optimization/README.md).

## Lifecycle bottleneck

Opt-in aggregate timings measured Start insertion, Scan's SQL and COMMIT phases, Complete's pool wait, transaction-row lock, line read, inventory updates, COMMIT or ROLLBACK, and handler time. The client JSON supplies end-to-end HTTP p95/p99; subtracting the corresponding handler means indicates roughly 0.4–1.6 ms of proxy, transport, and client work in representative runs. Phase means below are the final periodic cumulative profile snapshots, not exact end-of-run totals.

| Phase mean (ms) | Fixed 32 default | Fixed 32 stress | Growing default | Growing stress |
|---|---:|---:|---:|---:|
| Start pool wait / insert | 0.001 / 7.67 | 30.39 / 7.97 | <0.001 / 7.70 | 13.60 / 10.08 |
| Scan pool wait / COMMIT | <0.001 / 7.59 | 29.03 / 8.72 | <0.001 / 7.69 | 11.22 / 10.88 |
| Complete pool wait / inventory work | 0.001 / 5.74 | 28.71 / 48.93 | <0.001 / 6.28 | 11.21 / 124.03 |
| Complete COMMIT / ROLLBACK | 7.50 / none | 8.28 / 0.21 | 7.64 / none | 9.86 / 0.25 |
| Successful Complete store total | 14.12 | 127.56 | 14.77 | 258.47 |

At 10 stations, no pool queues formed. An auto-committed durable Start insert, every Scan COMMIT, and a successful Complete COMMIT each cost about 7.5–7.7 ms; repeated Scans therefore dominate basket time. At 100 stations with 32 connections, approximately 68 requests waited for the pool and each basket incurred about 10.5 Scans. Client Scan latency averaged 41.34 ms, or about 435 ms per started basket, versus 132.40 ms for a successful Complete. Pool queuing on repeated Scans was the primary limiter of client-counted transactions. The growing pool reduced Scan pool wait to 11.22 ms; client Scan latency fell to 26.40 ms (about 278 ms per basket). The bottleneck then shifted toward inventory row contention during Complete: successful Complete store time rose to 258.47 ms. More Scans also depleted popular stock sooner, raising expected insufficient-stock rejections. Materializer writes remained outside the Scan commit and did not block it through a checkpoint lock. Global InnoDB row-lock waits rose by 10,970 in the fixed-32 stress control and 15,252 in the retained stress run, while cumulative row-lock wait time rose by 1.01 million and 3.26 million ms; these are server-wide counters, so the timed inventory phase is the more direct evidence of checkout contention. `Innodb_log_waits` stayed at zero in both runs.

## Secondary metrics and selection

The representative fresh stress control and retained run are compared below; all Complete errors in the listed runs were HTTP 409 `INSUFFICIENT_STOCK`. Start and Scan had zero errors. SQL invariants passed after Materializer catch-up in **every valid run**.

| Metric | Fixed 32 before | Growing 32→64 after |
|---|---:|---:|
| `totalTransactions` | **12,282** | **14,164** |
| Successful Scans; scans/s | 223,171; 1,855 | 279,030; 2,321 |
| Insufficient-stock Complete rejections | 8,933 | 12,419 |
| Scan p95 / p99 | 91 / 152 ms | 62 / 106 ms |
| Successful Complete p95 / p99 | 331 / 392 ms | 727 / 867 ms |
| Sampled peak pending events / oldest age | 307 / 226 ms | 883 / 831 ms |
| Pending after catch-up; unexpected errors | 0; 0 | 0; 0 |

Across the retained three stress repeats, sampled pending peaks were 562–1,031 events and oldest ages were 220–831 ms; all drained to zero. These are two-second samples, not continuous maxima. Every valid run reconciled transaction totals, inventory decrements, accepted events, contiguous materialized positions, windows, and ranks through `db/validate-invariants.sql`.

Other isolated candidates:

| Candidate | Stress `totalTransactions` | Decision |
|---|---:|---|
| Fixed pool 64 | 14,443 / 14,552 / 14,570 | Strong stress result, but default trials were 4,304 / 4,704. Growing pool retained to preserve fixed-32 behavior under 10-station demand. |
| Fixed pool 96 | 15,385 / 15,784 | Rejected: Complete p95 exceeded 1.05 s and sampled Materializer backlog reached 14,577 / 24,617 events, oldest 3.4 / 5.6 s, versus under 0.46 s in fixed-64 repeats. It caught up after the client stopped, but the in-run lag was unacceptable for the modest extra count. |
| Read-only insufficient-stock precheck, fixed 32 | 12,162 | Rejected: below the fixed-32 control. |
| Precheck, fixed 64 | 14,876 / 14,407 / 14,567 | Rejected: small, inconsistent count change; no reliable gain. |
| Reverse inventory lock order, fixed 96 | 15,713 | Rejected: Complete p99 rose from 1.17 s to 1.82 s and maximum to 3.35 s for a small count gain. |

## Verification and reproduction

The focused bounded-pool concurrency test passed. The non-live repository suite passed 115 tests, with 29 live-only tests unavailable. The full suite through Nginx on the required public port 8080 then passed **143 tests, zero failures**, with only the opt-in destructive reset test aborted; see [live-tests-standard.txt](live-tests-standard.txt). A preliminary live run on port 18080 failed one test that explicitly requires 8080; it is excluded from verification. The full suite covers REST shapes and errors, durable completion, service isolation, and Materializer recovery. No Load Client file, SQL durability setting, or Materializer recovery rule changed.

Rebuild with `server/build.sh` and `load-client/build.sh`. To reproduce the retained default or stress run from `server/`, set `DB_USER` and `DB_PASSWORD`, then run:

```powershell
./benchmark-scan.ps1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe> -Variant retained -Workload default -TransactionPoolSize 32 -TransactionPoolMaxSize 64 -OutputLabel reproduce-default
./benchmark-scan.ps1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe> -Variant retained -Workload stress -TransactionPoolSize 32 -TransactionPoolMaxSize 64 -OutputLabel reproduce-stress
```

The harness resets only `cs6510_materializer_bench`, checks ports before starting, labels each result, samples lag and lock counters, waits for catch-up, and runs SQL invariant validation. The Transaction service default is the same growing 32→64 pool. Setting only `DB_POOL_SIZE` explicitly keeps a fixed pool for compatibility; set `DB_POOL_MAX_SIZE` as well to allow growth. Inventory and Analytics remain fixed at 10 by default.