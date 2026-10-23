# Microservices Architecture: Benchmark Evidence

Evidence for `specs/005-microservices-architecture`: three independently deployed services, each with its own MySQL schema and user, gRPC contracts between them, and a recoverable Saga checkout. The unchanged Load Client reaches them through Nginx.

**Headline**: under its mandatory durability rules, the microservices design (CP4) completes 11.3% fewer transactions than the service-based baseline (CP0) at default load, and 25.6% fewer under stress, measured interleaved on the same machine and evening (T152).
- **Default**: the gap is the price of two extra mandatory commits per basket, plus the outbox relay's commits.
- **Stress**: Inventory's row locks on the hot SKUs bound throughput.

Every performance claim here rests on client-observed `totalTransactions`. Statement counts, commit counts, and latencies only explain results (Implementation Rule 6).

## Official reports

| Report | Run | Command | `totalTransactions` | Errors | SHA-256 |
|---|---|---|---:|---|---|
| [default.json](default.json) | [`cp4-final-default-b-1`](cp4-final-default-b-1/) | `java -cp out Main` | 4326 | 0 on every operation | `629e2230a0b65a15f853d7b705628cac063a265643c9a588f295f8c40f1bed8b` |
| [stress.json](stress.json) | [`cp4-final-stress-b-1`](cp4-final-stress-b-1/) | `java -cp out Main --stations=100 --duration=120` | 11105 | Complete 6,633, all `409 INSUFFICIENT_STOCK`; 0 elsewhere | `b13ea84d39294ba403738e1638fb95b083fdde8dc990ad6cb0d7d6345f33ea66` |

- **Designation**: both runs were designated in advance as the first CP4 run of each workload in T152, and both were the first attempt of that run. Each file is a byte-for-byte copy of the client JSON in its run directory.
- **Each run**: it ran from `load-client/` after its own fresh reset of all three services (`db/reset-all.ps1`, with each service's own credentials), and it passed `db/validate-invariants.ps1` for Transaction, Inventory, and Analytics.
- **Stress errors**: the skewed Load Client sells out `SKU-000001`. Inventory answered all 17,738 `ApplyCheckout` calls with OK, applying 11,105 and rejecting 6,633. Nine per-call deadline expiries were retried within their requests.

### Completion counts (FR-054) and durability

| Run | COMPLETED | Client successes | Background completions | Completions without a recorded response | Remaining difference |
|---|---:|---:|---:|---:|---:|
| `cp4-final-default-b-1` | 4326 | 4326 | 0 | 0 | 0 |
| `cp4-final-stress-b-1` | 11105 | 11105 | 0 | 0 | 0 |

- **Durability**: both runs recorded `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and `log_bin=1` in `evidence.json`. No service or script changes a MySQL server variable.
- **Background completions**: there were none under stress either (FR-058). Every Complete settled within its request; the 8 s response budget never expired.

## Deployment and build

- **Machine**: Windows 10, JDK 21.0.2, MySQL 8.4.4 on `127.0.0.1:3307`, and Nginx 1.31.6 with the unchanged `deploy/nginx.conf` on port 8080.
- **Services**:

  | Service | HTTP port | gRPC port | Schema | Pool |
  |---|---:|---:|---|---|
  | Transaction | 8081 | — | `cs6510_transaction` | 32–64 |
  | Inventory | 8082 | 9092 | `cs6510_inventory` | 16–48 |
  | Analytics | 8083 | 9093 | `cs6510_analytics` | 10 |

  All configuration is at its documented default; CP3 changed none.
- **Source**: working copy on top of commit `f51e85d`. The official runs used the build that passed the final parity gate (T125), built with `server/build-tests.sh`, which runs `server/build.sh`.

| Artifact | SHA-256 |
|---|---|
| `transaction.jar` | `d94a4abbdb3ad73b00a327ef81c8715a6adbc822d32c875ba419e0b9ba731ddf` |
| `inventory.jar` | `2fcad28ef8532b1a63bf29255bbc2aba1281d0699b191122d144a883dc89ae65` |
| `analytics.jar` | `a01700cb2f602b2c1e2a628ad343c97495633ed58f7347ba8c49506ed8a96d67` |
| `common.jar` | `24f5024d864900cbd78fa553ed90ac2c4d555ec6247eda0d4b2a1c0381ab4eb7` |
| `contracts.jar` | `4814e766d2a40009180ebc304f9c9d801725d4fe219d00fc4450da8d3ee3f4e4` |

**Commands**, from the repository root, with `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, and `ANALYTICS_DB_PASSWORD` set, plus `DB_USER` and `DB_PASSWORD` for the CP0 baseline harness:

```powershell
# One measured run (reset, start, unchanged Load Client, drain, validate, evidence.json)
./server/benchmark-microservices.ps1 -Workload default -OutputLabel <label> -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe>

# The final interleaved comparison (T152): CP0 from a worktree of 26c89af against CP4
git worktree add ../cs6510-cp0 26c89af   # then build it with its server/build.sh and load-client/build.sh
./server/benchmark-compare.ps1 -Workload default -Label cp4-final-default -BaselineWorktree ../cs6510-cp0 -Pairs 3 -IdleCpuCores 2.0 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe>
```

The drivers that ran CP3 and CP4 are kept as [compare/cp3-campaign.ps1](compare/cp3-campaign.ps1), [compare/cp3-campaign-v1.ps1](compare/cp3-campaign-v1.ps1), and [compare/cp4-final.ps1](compare/cp4-final.ps1). The final test gate's output is [final-gate-tests.txt](final-gate-tests.txt): 378 tests, 377 successful, 0 failed, and only the opt-in destructive `ResetBaselineTest` aborted.

## Final comparison: CP0 against CP4 (T152)

Three interleaved pairs per workload, each run after its own reset, on 2026-10-22 between 21:01 and 21:41. CP0 is commit `26c89af` run through its own T001 harness. Three pairs describe the change; they do not test it, so no statistical significance is claimed. CP0 was ahead in every pair.

| Workload | Metric | CP0 | CP4 | Change |
|---|---|---:|---:|---:|
| default | **`totalTransactions`**, median (range) | **4879** (4861–4978) | **4326** (4261–4339) | **−11.3%** |
| default | per-pair Δ | | | −13.1%, −12.3%, −11.1% |
| default | completed transactions/s, median | 81.12 | 71.76 | −11.5% |
| default | error rate, every operation | 0% | 0% | — |
| default | Start p95 / p99 (ms) | 12.0 / 28.8 | 12.9 / 29.2 | |
| default | Scan p95 / p99 (ms) | 13.4 / 32.3 | 13.8 / 35.4 | |
| default | Complete p95 / p99 (ms) | 32.2 / 54.3 | 59.8 / 110.6 | |
| stress | **`totalTransactions`**, median (range) | **14916** (14296–14918) | **11105** (10413–11240) | **−25.6%** |
| stress | per-pair Δ | | | −22.3%, −30.2%, −24.7% |
| stress | completed transactions/s, median | 123.77 | 91.95 | −25.7% |
| stress | error rate: Start / Scan / Complete | 0% / 0% / 48.3% | 0% / 0% / 36.6% | Complete errors are all `409 INSUFFICIENT_STOCK` |
| stress | Start p95 / p99 (ms) | 60.9 / 103.8 | 21.0 / 55.7 | |
| stress | Scan p95 / p99 (ms) | 59.1 / 98.6 | 23.2 / 56.2 | |
| stress | Complete p95 / p99 (ms) | 692.5 / 797.8 | 1023.4 / 1458.2 | |

Latencies are client-side medians of each variant's three runs. The details and every run are in [checkpoints.md](checkpoints.md#cp4-final-cp0-versus-cp4-comparison-t152) and `compare/cp4-final-<workload>/compare.json`.

## Attribution: CP1–CP3

**These are attribution, not comparison.** CP1–CP3 ran at other stages and times, against the CP0 medians of T001: 4,759 at default and 14,375.5 under stress. They explain where cost was added; only the table above compares the designs. The full figures are in [checkpoints.md](checkpoints.md).

| | CP1: core checkout path, no Analytics | CP2: full system | CP3: tuning A/B |
|---|---|---|---|
| `totalTransactions` against CP0 | default 4,519–4,555 (−4.3% to −5.0%) | default 4,314–4,356 (−8.5% to −9.4%); stress 11,185 (−22.2%) | defaults' own runs: default 4,102–4,381; stress 10,416–11,131 |
| Pool borrow wait / hold time p99 (ms) | Transaction 0.05 / 27.9–30.4; Inventory 0.05 / 43.1–55.8 | default all 0.05 / 33–56; stress Inventory 893 / 751 | unchanged by every pool variant beyond noise |
| `TransactionLocks` wait p99 / contended / collisions | 0.05 ms / 90–92 / 89–92 | 0.05 ms / 76–950 / 75–950 | — |
| `inventory.apply.lockWait` p99 (ms) | 23.5–25.6 | 25.6–27.9 default; 751 stress | 531–1,263 under stress across variants |
| `transaction.rpc.applyCheckout` p95 / p99 (ms) | 25.6–27.9 / 43.1–55.8 | — | — |
| Relay backlog peak / unpublished-boundary peak | relay not wired | 32–126 / 1 | — |
| Visibility delay p99 (upper bound) | — | 237–305 ms, against a 5 s target | — |
| Recovery scan queries / wait database checks | 0 retries, budget expiries, or guard misses | 3–5 / 0 | — |
| Idle polls per second: relay / Analytics poller | — | 2.7–5.4 / 10.8–12.3 | — |
| Commits per completed transaction | 14.38–14.44 (N + 4) | 16.05–16.17 (N + 5.5) default; 23.86 stress | coalescing 15.99 → 14.67 at default, with no `totalTransactions` gain |

CP3 kept no variant. Server prepared statements, Inventory pools of 10/32 and 24/64, a Transaction pool of 48/80, and relay coalescing were all within noise or below +5% (checkpoints.md, *CP3*). The cache-off attribution run was skipped under the streamlined policy (research R22).

## Mandatory against optional cost (R18)

Per basket of N Scans (about 10.4 at default). CP4's figures are measured in `cp4-final-default-b-1`; CP0's commit count is by design, because its harness's commit counter does not change during its runs.

| Cost | CP0 | CP4 | Kind |
|---|---|---|---|
| Durable commits on the checkout path | N + 2: Start, N Scans, Complete | N + 4: Start, N Scans, T1, Inventory's apply, T3 | **Mandatory**: database per service (C-II) puts stock in Inventory's own commit, and a durable T1 must precede the remote step (C-V) |
| Remote calls | none | one `ApplyCheckout` per Complete, plus outbox delivery | **Mandatory** for Inventory; delivery is asynchronous and off the request path |
| Relay and ingestion commits | none (the materializer shared the database) | about 1.45 per basket at default (15.81 commits per completed transaction at 10.36 Scans) | Optional in batching only. Coalescing cut them by about 1.3 per basket but gained no measurable `totalTransactions` (CP3). |
| Per-request statements | not measured here | Scan: 2 statements + 1 commit, no SELECT; Complete: T1 2 + commit, apply 3 + commit, T3 2 + commit | Optional cost already at its design budget (`statement-budgets.properties`, unchanged) |

At about 7.5 ms per durable commit on this machine (R18), the two mandatory commits add about 15 ms to Complete. The measured Complete mean rose from about 16 to about 31 ms (CP1), and Complete p95 from 32 to 60 ms (CP4). With baskets of about 126 ms at CP0, that accounts for most of the 11.3% default gap; the relay's shared-instance commits account for the rest (CP2).

## Comparison with earlier reports

| Report | Architecture | Default `totalTransactions` (60 s, 10 stations) | Stress `totalTransactions` (120 s, 100 stations) |
|---|---|---:|---:|
| [service-based-architecture](../service-based-architecture/README.md) (2026-10-13) | Three services on one shared database; the first durable design | 999 | 2,120 |
| [transaction-optimization](../transaction-optimization/README.md) (2026-10-14) | The same shared database, with the SQL materializer and a growing 32→64 Transaction pool; this is CP0's configuration | 4,750–4,804 (median 4,751) | 13,897–14,304 (median 14,164) |
| This report (2026-10-22), CP0 runs | Commit `26c89af`, the end of the line above | 4,861–4,978 (median 4,879) | 14,296–14,918 (median 14,916) |
| This report (2026-10-22), CP4 | Database per service, gRPC, and the Saga | 4,261–4,339 (median 4,326) | 10,413–11,240 (median 11,105) |

- **Shared-database line**: the service-based design started at 999 and 2,120 because every Scan serialized on one sequence row. The SQL materializer and the growing pool raised that to about 4,750 and 14,160. CP0 reproduced it about 3–5% higher on the day of T152 (+2.7% at default, +5.3% under stress), which is why the final comparison uses fresh, interleaved CP0 runs instead of those older figures.
- **Microservices**: CP4 keeps CP0's durability: every acknowledged Scan and Complete is committed with `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`. It adds independent ownership, exactly-once settlement after interruptions, and exactly-once analytics. It gives up 11.3% of `totalTransactions` at default and 25.6% under stress, for the reasons above.
- **The in-memory pipeline**, before any durability (`../pipeline-architecture/`), remains far faster, because it acknowledged Scans from process memory. It is not comparable on durability.

## Evidence index

- **[checkpoints.md](checkpoints.md)**: every checkpoint, CP0–CP4, with its runs, metrics, and harness notes.
- **Run directories**: `cp*-*/` hold one run each: the byte-identical client report, `evidence.json`, `samples.csv`, `validator.txt`, and the service and gateway logs. The CP0 runs of T152 hold their harness's own outputs.
- **`compare/<label>/compare.json`**: every A/B comparison, with its runs, background CPU, invalidated attempts, and decision. The logs are `compare/cp3-campaign.log`, `compare/cp4-final.log`, and `compare/idle-gate.log`.
- **Excluded runs**, kept unaltered and never used: `compare/aborted-attempts/`, `compare/invalidated-machine-load/`, `compare/failed-port-conflict/`, `compare/superseded-by-streamlined-policy/`, each comparison's `invalidated/` and `interrupted/`, and `compare/cp4-final-default/failed-harness/`.
