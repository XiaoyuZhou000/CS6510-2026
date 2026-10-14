# SQL Materializer benchmark — 2026-10-13

The unchanged load client ran against three Java 21 services and Nginx on a separate reset MySQL 8.4 database, `cs6510_materializer_bench`, over the same host as the recorded commit-time sequence baseline. The public URL was port 18080 for this isolated run. No MySQL durability setting or load-client source was changed; `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1` were verified afterward.

| Workload | Prior Pipeline Architecture (scans/sec) | Scan throughput, old → new | Scan p95, old → new | Scan p99, old → new | Scan errors |
|---|---:|---:|---:|---:|---:|
| 10 stations, 60 s | 2,144.35 | 172.76 → **855.88/sec** (4.95×) | 74.10 → **13.82 ms** | 140.07 → **32.32 ms** | 0 |
| 100 stations, 120 s | 3,698.38 | 177.23 → **837.73/sec** (4.73×) | 1,446.49 → **215.67 ms** | 2,836.81 → **330.24 ms** | 0 |

The new run completed 51,538 Scans in 60.22 seconds and 101,167 Scans in 120.76 seconds. Default completed 4,951 transactions with no errors. Stress started 9,675 transactions; 8,506 completed and 1,169 completion requests received `409 INSUFFICIENT_STOCK`. Every line in the retained [stress error log](stress-client-errors.txt) has that code; the most popular SKU reached zero stock. This changes completed-transaction throughput comparability but does not indicate lost Scans. The unchanged client recorded no Scan errors.

| Workload | Materializer avg events/sec over sampled interval | Peak 2 s events/sec | Peak pending events | Peak oldest pending age | InnoDB row lock wait delta | Row lock time delta |
|---|---:|---:|---:|---:|---:|---:|
| Default | 748.23 | 925.41 | 18 | 43 ms | 2,268 | 21,326 ms |
| Stress | 780.28 | 940.44 | 70 | 135 ms | 3,967 | 43,862 ms |

The average Materializer rates include startup and post-client drain; the instantaneous rates come from the two-second [samples](samples.csv). The InnoDB counters are server-global deltas over each run, so they include checkout, completion, materialization, and other database activity; they do not isolate the Materializer checkpoint row. Pending-event lag drained to zero after each workload.

Both [default](default-invariants.txt) and [stress](stress-invariants.txt) database invariant checks passed. They independently reconciled basket lines, stock, all positioned events, every eligible 1,000/500 window, and top-ten ranks. The default ended with 51,538 positioned events and 102 complete windows; stress ended with 101,167 positioned events and 201 complete windows. The full live suite on port 8080 against the same new-schema layout passed [141 tests, zero failures](live-tests-standard-port.txt), with only the opt-in destructive reset test aborted. A post-suite [invariant run](live-invariants.txt) also passed. The one-time migration was verified on a disposable copy of the old position-primary-key table: two old positioned events remained positioned and a new event inserted with NULL position.

## Reproduce

Build the three service JARs and unchanged load client. With `DB_USER` and `DB_PASSWORD` set, run [benchmark-materializer.ps1](../../../server/benchmark-materializer.ps1) from `server/`, passing paths to MySQL CLI, Nginx, and Java 21. The script creates or resets only `cs6510_materializer_bench`, starts the three services on 18081–18083 and Nginx on 18080, runs the unchanged client with default 60-second and 100-station/120-second settings from separate fresh resets, samples lag and lock waits, validates invariants, and stops those processes. It writes raw JSON under [default](default/) and [stress](stress/). It requires ports 18080–18083 to be free.

The old baseline values above are from [the prior distributed evidence](../service-based-architecture/README.md). This is one controlled run per workload on this host, so the numbers establish a large improvement here but do not estimate run-to-run variance.
