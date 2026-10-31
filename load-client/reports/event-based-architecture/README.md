# Event-Based Architecture: Benchmark Evidence

Evidence for `specs/006-event-based-architecture`: the three microservices of feature 005, each with its own MySQL schema and user, now exchange only domain events through one Apache Kafka broker. No service calls another. Every event is written to its producer's outbox in a local transaction and is applied at most once by its consumer. The unchanged Load Client reaches the services through the unchanged Nginx gateway. The architecture diagram is [docs/architecture/event-based-architecture.md](../../../docs/architecture/event-based-architecture.md), with an SVG export beside it ([event-based-architecture.svg](../../../docs/architecture/event-based-architecture.svg)).

**Headline**: measured interleaved on the same machine and in the same hour (T166), the event-based design completes 7.5% fewer transactions than the feature 005 release at default load (4560 against 4930), and 6.1% fewer under stress (10725 against 11423). The two broker hops are the cost of durable, decoupled messaging: each hop waits for an `acks=all` confirmation, a consumer fetch, and a commit on the consuming side. Under stress, the hot SKU's row lock bounds both designs.

Every performance claim here rests on the client-observed `totalTransactions`. Latencies, lags, and commit counts only explain results.

## Official reports

The unchanged Load Client's JSON cannot carry settings or a deployment identity (FR-062). Each committed report is therefore identified here by its run and by the run directory's `evidence.json`, which records the git commit, the jar hashes, the broker version and flush setting, the configuration, and the durable counts.

| Report | Run | Command | `totalTransactions` | Errors | Git commit | Settings | Broker | SHA-256 |
|---|---|---|---:|---|---|---|---|---|
| [default-report.json](default-report.json) | [`cp4-default-1`](cp4-default-1/) ([evidence.json](cp4-default-1/evidence.json)) | `java -cp out Main` | 4371 | 0 on every operation | `2ac2c07` + uncommitted phase 10 documentation (`gitDirty: true`) | every default, `PERF_METRICS=true` | Kafka 4.1.0, KRaft, native Windows, flush at broker default (page cache) | `1b34cc694c784ae29ea6b7f179ad4a2c29c4be5dbea58a7de19b8f1082e509e2` |
| [stress-report.json](stress-report.json) | [`cp4-stress-1`](cp4-stress-1/) ([evidence.json](cp4-stress-1/evidence.json)) | `java -cp out Main --stations=100 --duration=120` | 10553 | Complete 5954, all `409 INSUFFICIENT_STOCK`; 0 elsewhere | same | same | same | `680d2dd54b2f6231d594fb54aa899a91e86d961438ef12c54e611c2efe1dc0b6` |

- **Designation**: both runs were designated in advance in [checkpoints.md](checkpoints.md#cp4-designation-t164t166) as the first CP4 run of each workload. Both were first attempts. Each committed file is a byte-for-byte copy of the client report in its run directory, verified by SHA-256 against the copy and against `evidence.json`'s `client.reportSha256`.
- **Each run**: started after its own fresh baseline (`db/reset-baseline.ps1`), which empties the broker's topics and offsets and resets all three schemas. Then `db/validate-invariants.ps1` passed for Transaction, Inventory, and Analytics, and the cross-service reconciliation passed, after all work had settled.
- **Diagram commit**: the diagram, this README, and the copied reports were committed together after the runs. The phase 9 code the runs used was unchanged.

### Per-operation results

| Run | Operation | Successes | Errors | Error rate | p95 (ms) | p99 (ms) |
|---|---|---:|---:|---:|---:|---:|
| default | Start | 4371 | 0 | 0% | 12.1 | 27.6 |
| default | Scan | 45746 | 0 | 0% | 13.6 | 41.8 |
| default | Complete | 4371 | 0 | 0% | 71.0 | 140.0 |
| stress | Start | 16507 | 0 | 0% | 23.4 | 60.7 |
| stress | Scan | 174096 | 0 | 0% | 26.0 | 66.9 |
| stress | Complete | 10553 | 5954 | 36.1% | 1427.1 | 2671.6 |

Throughput: 72.7 completed transactions per second at default (60.1 s) and 87.6 under stress (120.5 s). The popular-items ranking is stable: SKU-000001, -000002, and -000003 hold ranks 1–3 in both reports, as in feature 005's reports.

### Durable counts (FR-061) and invariants

| Run | COMPLETED (durable) | Client `totalTransactions` | Background completions | Completions without a recorded response | Remaining difference | Parked events | Final consumer lag |
|---|---:|---:|---:|---:|---:|---:|---:|
| `cp4-default-1` | 4371 | 4371 | 0 | 0 | 0 | 0 | 0 |
| `cp4-stress-1` | 10553 | 10553 | 0 | 0 | 0 | 0 | 0 |

- **SC-013**: the default run recorded zero errors for each operation, zero background completions, and zero dead-lettered messages.
- **Background completions under stress (FR-066)**: there were none. Every Complete settled within its request, although Inventory's checkout consumer fell behind the hot SKU's row lock: consumer lag p95 1378 ms, p99 2527 ms, and lock wait p99 223 ms. Transaction's outcome wait (p99 2527 ms) stayed well inside the 8 s `CHECKOUT_RESPONSE_BUDGET_MS`.
- **Invariants** (`validator.txt` in each run directory): stock units plus applied deductions match the baseline. Each completed transaction has exactly one applied operation. Every accepted scan reached Analytics exactly once (`stored_events` = `last_position` = accepted scans). All outboxes are empty, nothing is parked, and every consumer group is stable at lag 0.
- **Durability**: both runs recorded `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and `log_bin=1`. The producers use `acks=all` with idempotence. Nothing is lost if the broker loses its page cache: an outbox row is marked published only after confirmation, and anything unconfirmed is sent again.

## Deployment and build

- **Machine**: Windows 10, JDK 21.0.2, MySQL 8.4.4 on `127.0.0.1:3307`, Nginx 1.31.6 with the unchanged `deploy/nginx.conf` on port 8080. Docker is not used.
- **Broker**: Apache Kafka 4.1.0 in KRaft mode, one native node at `127.0.0.1:19092` (`deploy/kafka/`), on the same JDK. Its flush setting is the broker default: `KAFKA_FLUSH_EVERY_MESSAGE` is unset, so `log.flush.interval.messages` is not set. Four topics: `selfcheckout.catalog.v1` (1 partition), `selfcheckout.checkout.v1` (16), `selfcheckout.checkout-outcome.v1` (16), and `selfcheckout.scan-accepted.v1` (1).
- **Services**:

  | Service | HTTP port | Schema | Consumes | Produces |
  |---|---:|---|---|---|
  | Transaction | 8081 | `cs6510_transaction` | outcomes (`transaction-checkout-outcome`, 8 threads), catalog (replay) | checkout requests, scans |
  | Inventory | 8082 | `cs6510_inventory` | checkout requests (`inventory-checkout`, 8 threads) | outcomes, catalog |
  | Analytics | 8083 | `cs6510_analytics` | scans (`analytics-scan-ingest`, 1 thread) | — |

- **Configuration**: every variable at its documented default (`server/README.md`, *Configuration*). CP3 tested four options and adopted none (see *Attribution* below).
- **Artifacts** (built by `server/build-tests.sh` → `server/build.sh` at the phase 9 gate; identical in all CP3 and CP4 runs):

| Artifact | SHA-256 |
|---|---|
| `transaction.jar` | `5f36ea18d07f8faa62cd1f7de5084430f631a93d6286f945d020e8226e5a7fd4` |
| `inventory.jar` | `42825bc7155a1dede058da1453860ec6054f603afd35469be1f31541938ec22f` |
| `analytics.jar` | `fbac89aa88f8b4cab08a1e42cfbd96a8a724a7b85df3c7ea60bbdcf654aaf764` |
| `common.jar` | `cbb9447e4880b90db692b9f160453069b81689e4f8aa214a21bbbc452b4bc170` |
| `contracts.jar` | `eb1258d57dc83f2daeb06813fe786d3f6ebd75b0e383a604f45314187fe77888` |

**Commands**, from the repository root. They need `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, and `ANALYTICS_DB_PASSWORD`, plus `KAFKA_HOME` for the broker scripts, with the broker started by `deploy/kafka/start-broker.ps1`:

```powershell
# One official run: fresh baseline (broker topics and all three schemas), start, unchanged Load Client, drain, validate, evidence.json
./server/benchmark-event-based.ps1 -Workload default -OutputLabel cp4-default-1 -NginxBin <nginx.exe> -MySqlExe <mysql.exe> -JavaExe <JDK21-java.exe> -IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights
./server/benchmark-event-based.ps1 -Workload stress -OutputLabel cp4-stress-1 <same options>

# The final interleaved comparison with feature 005 (T166); builds nothing, so build ../cs6510-baseline-8891c57/server first
./server/benchmark-compare.ps1 -Workload default -Label cp4-vs-cp0-default -BaselineCommit 8891c57 -Pairs 1 <same options>
```

## Final comparison: CP0 against CP4 (T166)

One interleaved pair per workload on 2026-10-31 between 04:56 and 05:09, each run after its own reset, on an idle machine (background CPU 0.08–0.28 cores). CP0 is commit `8891c57`, the feature 005 release, run by its own harness from a clean worktree (`gitDirty: false`). One pair describes the change. It does not test it, so no significance is claimed.

| Workload | Metric | CP0 (feature 005) | CP4 (feature 006) | Change |
|---|---|---:|---:|---:|
| default | **`totalTransactions`** | **4930** | **4560** | **−7.5%** |
| default | completed transactions/s | 81.8 | 75.8 | −7.4% |
| default | error rate, every operation | 0% | 0% | — |
| default | Start p95 / p99 (ms) | 11.2 / 23.4 | 11.6 / 32.3 | |
| default | Scan p95 / p99 (ms) | 12.4 / 32.6 | 13.6 / 42.5 | |
| default | Complete p95 / p99 (ms) | 53.3 / 101.4 | 71.4 / 137.8 | |
| stress | **`totalTransactions`** | **11423** | **10725** | **−6.1%** |
| stress | completed transactions/s | 94.7 | 88.9 | −6.1% |
| stress | error rate: Start / Scan / Complete | 0% / 0% / 38.9% | 0% / 0% / 36.8% | Complete errors are all `409 INSUFFICIENT_STOCK` |
| stress | Start p95 / p99 (ms) | 18.5 / 50.0 | 24.1 / 63.5 | |
| stress | Scan p95 / p99 (ms) | 20.7 / 49.9 | 26.5 / 69.6 | |
| stress | Complete p95 / p99 (ms) | 969.8 / 1386.3 | 1524.9 / 2067.1 | |

Both CP0 runs recorded zero background completions and both CP4 runs zero parked events and zero final lag. The details are in [checkpoints.md](checkpoints.md#cp4-official-runs-and-the-final-comparison-t164t166) and `compare/cp4-vs-cp0-<workload>/compare.json`.

**Why CP4 is slower**:
- **Default**: Complete's synchronous call to Inventory became two event hops. Each hop is a produce with `acks=all` to the local broker, a consumer fetch, and the consumer's commit. Complete p95 rose by 18 ms. Commits on the request path are unchanged (N + 4 per basket: Start, N Scans, T1, Inventory's apply, T3). The outbox INSERTs ride inside T1 and the apply. The dispatchers add off-path commits that mark rows published, and those commits share the redo-log and binlog flushes with Start and Scan. That is why the unchanged Start and Scan paths also lost a little at p99.
- **Stress**: the hot SKU's row lock bounds both designs. With events, an apply waits behind the earlier applies of its partition, where feature 005 had each request wait for its own RPC. So Complete p95 rose more than throughput fell.

## Attribution: CP1–CP3

**These are attribution, not comparison.** They ran at other times against the CP0 medians of 2026-10-29 (4816.5 default, 11427 stress). The full record, with each investigation, is in [checkpoints.md](checkpoints.md).

| Checkpoint | Change | Default `totalTransactions` (Δ vs CP0) | Stress |
|---|---|---|---|
| CP1 | Checkout Saga over events; catalog and scans still gRPC | 4312–4397 (−9.6% median) | 10711 (−6.3%) |
| CP2 | Every interaction over events | 4021–4043 (−16.3%), loaded by a background game | 10847 (−5.1%) |
| CP3 | Defaults' own runs in the A/B campaign, idle machine | 4180–4559 (median 4445.5, −7.7%) | 10231–10936 (median 10583.5, −7.4%) |
| CP4 | Official runs and the T166 pair | 4371–4560 | 10553–10725 |

**CP3** (T163, one screening pair each, D15's streamlined policy):

| Option | Workload | Default → variant | Δ `totalTransactions` | Decision |
|---|---|---|---:|---|
| `KAFKA_FLUSH_EVERY_MESSAGE` | default | false → true | −29.9% | keep false (D17's ±3% criterion not met) |
| `KAFKA_FLUSH_EVERY_MESSAGE` | stress | false → true | −16.1% | keep false |
| `CHECKOUT_CONSUMER_THREADS` | default | 8 → 4 | +2.2% | keep 8 (within ±3%) |
| `CHECKOUT_CONSUMER_THREADS` | stress | 8 → 4 | +4.1% | keep 8 (below +5%, no confirmation round) |
| `SCAN_PRODUCER_LINGER_MS` | default | 5 → 20 | −3.2% | keep 5 |
| `OUTBOX_COALESCE_MS` | default | 0 → 100 | −4.2% | keep 0 |

## Comparison with earlier reports

| Report | Architecture | Default `totalTransactions` (60 s, 10 stations) | Stress `totalTransactions` (120 s, 100 stations) |
|---|---|---:|---:|
| [service-based-architecture](../service-based-architecture/README.md) (2026-10-13) | Three services on one shared database | 999 | 2,120 |
| [transaction-optimization](../transaction-optimization/README.md) (2026-10-14) | The same shared database with the SQL materializer | median 4,751 | median 14,164 |
| [microservices-architecture](../microservices-architecture/README.md) (2026-10-22), committed reports | Database per service, gRPC, recoverable Saga | 4,326 | 11,105 |
| This report, T166 CP0 run (2026-10-31) | The same feature 005 release, `8891c57`, measured again | 4,930 | 11,423 |
| This report, official runs (2026-10-31) | Database per service, Kafka events, outboxes, choreographed Saga | 4,371 | 10,553 |

- **Against feature 005's committed reports**: the official default run is 1.0% above `microservices-architecture/default.json` (4371 against 4326), and the stress run is 5.0% below `stress.json` (10553 against 11105). Those figures come from different days. The same feature 005 release measured 4930 and 11423 on the day of T166, about 14% and 3% above its committed reports, so the design comparison uses only the interleaved T166 pair: −7.5% and −6.1%.
- **What the cost buys**: no service-to-service call remains (US7, `MicroserviceBoundaryTest`). Each service starts, runs, and restarts without its peers. Scan stays durable and answers while the broker is down (US2). Checkout settles exactly once despite duplicated, delayed, or lost messages and broker restarts (US3). Analytics receives every scan exactly once in one total order (US5). The public contract is unchanged (US6).

## Evidence index

- **[checkpoints.md](checkpoints.md)**: every checkpoint, CP0–CP4, with its runs, metrics, investigations, and harness notes.
- **Run directories** `cp*-*/` hold one run each: the byte-identical client report, `evidence.json`, `samples.csv`, `lag-samples.csv`, `kafka-status-final.json`, `validator.txt`, and `reset-baseline.txt`. The CP0 runs of T166 (`cp4-vs-cp0-*-a-1`) hold their own harness's outputs. Service and gateway logs (`*.log`) stay local, because `.gitignore` excludes them.
- **`compare/<label>/compare.json`**: every A/B comparison with its runs, background CPU, and decision. `compare/idle-gate.log` records every idle-gate wait. Runs that were set aside are kept unaltered and were never used: `compare/cp3-threads-default/interrupted/` (stopped when the campaign was paused) and `compare/cp3-threads-default/invalidated/` (a game ran during it).
- **Diagram**: [docs/architecture/event-based-architecture.md](../../../docs/architecture/event-based-architecture.md) (Mermaid, rendered by GitHub) and its [SVG export](../../../docs/architecture/event-based-architecture.svg). `server/tests/architecture/ArchitectureDiagramConsistencyTest` checks both against the event catalog.
