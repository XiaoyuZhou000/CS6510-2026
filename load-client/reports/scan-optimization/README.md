# Scan path profiling and controlled optimization — 2026-10-13–14 (Pacific)

The unchanged load client ran through Nginx against three independently launched Java 21 services and MySQL 8.4 on port 3307. Every listed workload started with a separate full reset of the disposable `cs6510_materializer_bench` database. The Transaction pool was the only service configuration changed in the retained candidate: its default size is now 32 instead of 10; Inventory and Analytics remain at 10. `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1` remained enabled. `SCAN_PROFILE=true` collected aggregate timings without request data. The original [SQL Materializer report](../sql-materializer/README.md) remains intact.

## Retained A/B result

| Configuration | Workload | Scans/sec | Scan p50 / p95 / p99 (ms) | Completed/sec | Expected Complete 409s | Scan errors | Invariants |
|---|---|---:|---:|---:|---:|---:|---|
| Pool 10 baseline | 10 stations, 60 s | 825.68 | 8.50 / 14.09 / 31.65 | 78.48 | 0 | 0 | PASS |
| Pool 32 first | 10 stations, 60 s | 823.01 | 8.44 / 14.76 / 35.64 | 78.83 | 0 | 0 | PASS |
| Pool 32 retained | 10 stations, 60 s | 815.17 | 8.55 / 14.58 / 34.60 | 76.91 | 0 | 0 | PASS |
| Pool 10 baseline | 100 stations, 120 s | 844.20 | 71.50 / 212.46 / 319.28 | 69.80 | 1,306 | 0 | PASS |
| Pool 32 first | 100 stations, 120 s | 1,761.50 | 36.36 / 93.76 / 156.51 | 98.88 | 8,288 | 0 | PASS |
| Pool 32 repeat | 100 stations, 120 s | 1,729.94 | 36.78 / 96.14 / 169.84 | 97.26 | 8,172 | 0 | PASS |
| Pool 32 retained | 100 stations, 120 s | 1,816.63 | 35.26 / 91.74 / 153.53 | 100.42 | 8,715 | 0 | PASS |

The stress improvement repeated at 2.05–2.15 times baseline throughput and roughly halved Scan p95/p99. The 10-station workload showed **no reproducible throughput or tail-latency improvement**; the earlier unprofiled SQL Materializer run measured 855.88 scans/sec, so this work does not establish a new default-workload high. More successful Scans exhausted popular inventory sooner in stress runs. Every retained-run client error line was `409 INSUFFICIENT_STOCK` from Complete; there were no unexpected errors. The raw client reports, recorded profile snapshots, available two-second MySQL samples, and SQL invariant results are under [baseline](baseline/), [candidate](candidate/), [repeat](repeat/), and [retained](retained/).

## Measured bottleneck

The timed Scan path in `JdbcAcceptedScanStore` includes pool acquisition, transaction-row `FOR UPDATE`, catalog lookup, line upsert and captured-price read, full basket aggregation, total update, raw-event insert, commit, and connection reset/return. The handler measures successful request and response-write time. The histogram is cumulative per process; the phase means below come from the latest ten-second snapshot, which can precede the end of a run. Its 0.1 ms bins cap reported percentiles at 300.1 ms, while the unchanged client JSON gives uncapped end-to-end latency.

| Timed phase, mean ms | Pool 10 baseline default | Pool 10 baseline stress | Pool 32 retained default | Pool 32 retained stress |
|---|---:|---:|---:|---:|
| Pool acquisition | <0.001 | 87.65 | <0.001 | 29.68 |
| Transaction-row lock query | 0.15 | 0.18 | 0.16 | 0.25 |
| Catalog lookup | 0.13 | 0.15 | 0.13 | 0.22 |
| Line upsert | 0.22 | 0.27 | 0.24 | 0.31 |
| Captured-price read | 0.14 | 0.14 | 0.14 | 0.20 |
| Basket aggregation | 0.16 | 0.16 | 0.16 | 0.22 |
| Total update | 0.18 | 0.21 | 0.19 | 0.26 |
| Raw-event insert | 0.25 | 0.33 | 0.29 | 0.40 |
| Commit | 7.94 | 8.13 | 7.97 | 9.03 |
| Connection reset and return | not separately recorded | not separately recorded | 0.10 | 0.15 |
| Transaction execution, through commit | approximately 9.3 | approximately 9.8 | 9.38 | 11.07 |
| Successful request, including response write | approximately 9.4 | 97.50 | 9.53 | 40.99 |

At 100 stations the pool of 10 was fully occupied with approximately 88 waiters; pool wait dominated client latency. At 10 stations, the pool had no waiters and durable commit dominated. Raising the Transaction pool to 32 left 68 waiters under stress but allowed more independent Scan commits in flight. It did not remove the roughly 8–9 ms mean commit cost. The measured SQL reads each cost a fraction of a millisecond. The handler response write was about 0.04 ms at 10 stations and 0.08 ms at 100 stations.

Sampled MySQL maximum connected/running threads were 31/14 with pool 10 stress and 53/36 with pool 32 stress, below `max_connections=151`. `Innodb_log_waits` stayed zero; log fsync counts rose by 61,690 and 58,676 during those runs. Global row-lock waits rose by 3,812 and 10,603, respectively, across more than twice as many Scans plus other writes; the directly timed transaction-row query stayed below 0.3 ms mean. OS process CPU was inaccessible to the test account, and the fsync counter does not by itself prove disk saturation. The commit timing and pool occupancy are the strongest evidence. MySQL durability settings were verified as 1/1.

The Materializer still owns the only `scan_sequence` lock and never places it on Scan. Over the sampled intervals, materialized-event rates including startup and post-client drain were 790.45/sec for pool-10 stress, 1,664.78/sec and 1,632.61/sec for the first two pool-32 stress runs, and 1,714.68/sec for retained stress. The retained default rate was 724.19/sec over 67.7 sampled seconds. Each run ended with zero pending events. The two-second samples and aggregate batch logs are preserved beside each client report.  In the retained default run its last profiled 5,000 batches averaged 9.22 events, 0.133 ms checkpoint lock query, 1.48 ms row updates, 8.12 ms commit, and 10.21 ms total per batch. In retained stress its last profiled 8,000 batches averaged 24.78 events, 0.129 ms checkpoint query, 3.40 ms row updates, 8.61 ms commit, and 12.99 ms total. The retained two-second samples saw pending peaks of 331 default and 635 stress, oldest ages of 440 and 524 ms, and zero pending after catch-up. These are sampled maxima, not continuous upper bounds. Both runs reconciled every raw event, analytics position, complete 1,000/500 window, deterministic rank, basket total, and inventory decrement.

## Candidate decisions

| Candidate | Expected benefit | Correctness risk / effort | Evidence and decision |
|---|---|---|---|
| Transaction pool 10 → 32 | High at 100 stations; none at 10 | Low / low | **Kept.** Acquisition wait fell from 87.65 to 29.68 ms mean; stress throughput doubled in three fresh runs. |
| Join captured line price into catalog read | Low: one roughly 0.14–0.21 ms round trip | Medium / low | Rejected. `sql` gave 837.44 default and 1,796.52 stress; default repeat fell to 825.38 with worse tails. Source reverted; captured-price regression test retained. |
| Pause 5 ms after Materializer batches below 32 | Possible log-commit savings | Low correctness risk, freshness risk / low | Rejected. Stress trials gave 1,837.30, 1,804.50, then 1,704.75 scans/sec; sampled pending backlog reached 1,258 and age 669 ms. Delay reverted. |
| Drop `idx_scan_pending` | Possible secondary-index write savings | Low / low | Rejected. `EXPLAIN` used `uq_scan_position` even before removal and continued to do so after. Trial gave 843.46 default and 1,737.29 stress, without a clear gain. Production schema unchanged. The earlier pool-32 runs in [retained-index-dropped](retained-index-dropped/) unintentionally combined the pool change with dropping this index; they are excluded from the retained A/B comparison. The corrected retained runs verified the index present, and the benchmark harness now rejects index-drop variants. |
| Persist an incremental item count and running total | At most a small Scan SQL saving at current basket sizes | High schema/migration and invariant risk / medium | Not implemented. The transaction-row lock would make `count+1` and `total+captured_unit_price` safe in one commit, but a new persisted count must be backfilled and checked against lines. Basket aggregation currently costs only 0.16–0.22 ms versus 7.75–9.27 ms commit. Reprofile before undertaking this migration. |

No REST route, JSON shape, load-client source, MySQL durability setting, raw-event atomicity, Materializer position/checkpoint rule, Analytics window/rank behavior, or service boundary changed. The new regression test changes catalog price between two Scans of the same SKU and confirms the first captured price remains in the line, events, and response total.

## Verification and operating notes

The final full live suite passed **142 tests, zero failures**, with only opt-in `ResetBaselineTest` aborted, through Nginx and the three separate services; see [live-tests-retained.txt](live/live-tests-retained.txt). Every fresh benchmark workload passed [its SQL invariant check](retained/stress/stress-invariants.txt). The shared `cs6510_selfcheckout` database initially had the pre-Materializer table layout, so it was backed up locally at `tmp/cs6510-selfcheckout-before-sql-materializer-20261013.sql`, then migrated with the documented script. All 22,152 pre-existing events and the sequence checkpoint were preserved. A separate invariant check on that shared database found SKU-000042 stock reconciliation off by two; restoring the pre-migration backup into an isolated temporary database reproduced the same discrepancy, so it predates this optimization. Fresh benchmark databases have no such discrepancy.

Reproduce one workload from `server/` after building JARs and the unchanged client: `./benchmark-scan.ps1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <java.exe> -Variant retained -Workload default`, then use `-Workload stress`. Set `DB_USER` and `DB_PASSWORD` in the environment. The script resets **only** `cs6510_materializer_bench`, runs Nginx on 18080 and the three services on 18081–18083, samples MySQL, waits for Materializer catch-up, and checks invariants. Ports 18080–18083 must be free. The retained implementation defaults to pool 32 for Transaction Service and 10 for the other services; `DB_POOL_SIZE` can still override the size for each process. Set `SCAN_PROFILE=true` to emit aggregate phase lines; the benchmark does so automatically.
