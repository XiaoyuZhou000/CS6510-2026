# Benchmark Checkpoints: Microservices Architecture

Checkpoint record for `specs/005-microservices-architecture` (research R22). Every row reports the client-observed `totalTransactions` and its change against the CP0 median of the same workload. Fewer statements or lower latency never count as an improvement on their own.

- **CP0–CP3** attribute costs during implementation.
- **Final comparison**: only the fresh, interleaved CP0 and CP4 runs (T152) compare the designs.

## Results

Latencies are client-side p95 / p99 in milliseconds, from the Load Client JSON report. `Innodb_row_lock_waits` Δ is the change between the first and last harness sample of the run.

| Checkpoint | Label | Workload | `totalTransactions` | Δ vs CP0 median (%) | Start p95 / p99 | Scan p95 / p99 | Complete p95 / p99 | Complete errors | `Innodb_row_lock_waits` Δ | `innodb_flush_log_at_trx_commit` | `sync_binlog` | Harness output |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| CP0 | `cp0-default-1` | default | 4850 | +1.9 | 12.1 / 25.8 | 13.8 / 32.5 | 31.9 / 51.9 | 0 | 2237 | 1 | 1 | [cp0-default-1](cp0/cp0-default-1/default/) |
| CP0 | `cp0-default-2` | default | 4743 | −0.3 | 12.3 / 29.5 | 14.6 / 34.7 | 34.4 / 61.2 | 0 | 2197 | 1 | 1 | [cp0-default-2](cp0/cp0-default-2/default/) |
| CP0 | `cp0-default-3` | default | 4759 | 0.0 | 12.2 / 26.4 | 14.6 / 36.6 | 34.2 / 63.4 | 0 | 2267 | 1 | 1 | [cp0-default-3](cp0/cp0-default-3/default/) |
| CP0 | `cp0-stress-1` | stress | 14421 | +0.3 | 61.1 / 107.4 | 60.2 / 101.8 | 715.7 / 817.0 | 13207 | 14906 | 1 | 1 | [cp0-stress-1](cp0/cp0-stress-1/stress/) |
| CP0 | `cp0-stress-2` | stress | 14330 | −0.3 | 59.6 / 104.5 | 60.0 / 99.4 | 719.0 / 787.9 | 13298 | 15258 | 1 | 1 | [cp0-stress-2](cp0/cp0-stress-2/stress/) |
| CP1 | `cp1-default-1` | default | 4555 | −4.3 | 11.3 / 20.3 | 12.8 / 27.6 | 50.9 / 96.2 | 0 | 1533 | 1 | 1 | [cp1-default-1](cp1-default-1/) |
| CP1 | `cp1-default-2` | default | 4519 | −5.0 | 11.5 / 25.2 | 13.1 / 30.6 | 58.3 / 100.4 | 0 | 1518 | 1 | 1 | [cp1-default-2](cp1-default-2/) |
| CP2 | `cp2-default-1` | default | 4356 | −8.5 | 12.0 / 25.4 | 13.4 / 34.8 | 53.6 / 105.7 | 0 | 1492 | 1 | 1 | [cp2-default-1](cp2-default-1/) |
| CP2 | `cp2-default-2` | default | 4314 | −9.4 | 12.3 / 31.5 | 13.7 / 39.7 | 56.6 / 114.7 | 0 | 1445 | 1 | 1 | [cp2-default-2](cp2-default-2/) |
| CP2 | `cp2-stress-1` | stress | 11185 | −22.2 | 21.1 / 51.1 | 22.8 / 55.7 | 1058.8 / 1422.4 | 6443 | 16944 | 1 | 1 | [cp2-stress-1](cp2-stress-1/) |

## Summary

| Checkpoint | Workload | Runs | `totalTransactions` median | Range (min–max) |
|---|---|---:|---:|---|
| CP0 | default | 3 | 4759 | 4743–4850 |
| CP0 | stress | 2 | 14375.5 | 14330–14421 |
| CP1 | default, without Analytics | 2 | 4537 | 4519–4555 |
| CP2 | default | 2 | 4335 | 4314–4356 |
| CP2 | stress | 1 | 11185 | 11185 |
| CP4 (T152, interleaved with fresh CP0) | default | 3 | 4326 | 4261–4339 |
| CP0 (T152, fresh) | default | 3 | 4879 | 4861–4978 |
| CP4 (T152, interleaved with fresh CP0) | stress | 3 | 11105 | 10413–11240 |
| CP0 (T152, fresh) | stress | 3 | 14916 | 14296–14918 |

## CP0: attribution baseline (T001)

**Measured**: 2026-10-20, 21:10–21:19 local time, five back-to-back runs on the benchmark machine.

**Implementation**: the unmodified service-based implementation at commit `26c89af`, built with `server/build.sh` and `load-client/build.sh` before any other task of this feature.

**Command**: from `server/`, with `DB_USER` and `DB_PASSWORD` set:

```powershell
./benchmark-scan.ps1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe> -Variant retained -Workload default -TransactionPoolSize 32 -TransactionPoolMaxSize 64 -OutputLabel cp0-default-1
```

The other runs use the labels `cp0-default-2` and `cp0-default-3`, and `-Workload stress` with `cp0-stress-1` and `cp0-stress-2`.

**Environment**: Windows 10, JDK 21.0.2, MySQL 8.4.4 on `127.0.0.1:3307`, and Nginx 1.31.6 on the harness's disposable ports 18080–18083. The server variables were the same before and after the runs: `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and `log_bin=1`. No server variable was changed.

**Harness notes**:
- **Output location**: `benchmark-scan.ps1` writes labelled runs to `load-client/reports/transaction-optimization/<label>/`. The five CP0 directories were moved unchanged to `cp0/<label>/` here, so the feature's evidence stays in one folder.
- **Harness fix**: the first attempt at `cp0-default-1` completed its load (4,746 transactions, zero errors). Then the harness aborted with "Load client failed" before draining and validation. Under Windows PowerShell 5.1, `Start-Process -PassThru` returns a process whose `ExitCode` reads as null unless its handle is opened before exit. One line, `$null = $clientProc.Handle`, was added to `server/benchmark-scan.ps1`. It changes no measured code or setting. The aborted attempt is excluded, and all five runs above were then taken in sequence.

**Validation**: `db/validate-invariants.sql` returned `PASS` for every run. The materialized position matched the accepted units in every run: default 50,890, 50,392, and 50,185; stress 290,360 and 290,739.

**Errors**:
- **Default**: zero errors on every operation.
- **Stress**: every Complete error is the business rejection `409 INSUFFICIENT_STOCK`. The skewed Load Client sells out the hottest SKUs; the minimum stock reaches 0, and the nonnegative-stock invariant still passes.
- **History**: earlier runs with the same 32→64 pool show the same pattern: `transaction-optimization/grow32to64-stress-*`, 13,897–14,304 transactions with 12,419–12,934 such rejections.

**Server-side Transaction pool wait**: from the harness's `SCAN_PROFILE` output in `transaction-stderr.log`, which is the last periodic cumulative summary before the services were stopped. For attribution only. Service logs are git-ignored (`*.log`), so this table preserves their figures.

| Label | Start pool p99 / max (ms) | Scan pool p99 / max (ms) | Complete pool p99 / max (ms) | Max pool size | Max active | Max waiters |
|---|---:|---:|---:|---:|---:|---:|
| `cp0-default-1` | 0.0 / 0.049 | 0.0 / 0.176 | 0.0 / 0.041 | 32 | 10 | 0 |
| `cp0-default-2` | 0.0 / 0.049 | 0.0 / 0.341 | 0.0 / 0.059 | 32 | 10 | 0 |
| `cp0-default-3` | 0.0 / 0.029 | 0.0 / 0.196 | 0.0 / 0.024 | 32 | 10 | 0 |
| `cp0-stress-1` | 73.8 / 819.1 | 63.2 / 819.8 | 62.8 / 818.8 | 64 | 64 | 36 |
| `cp0-stress-2` | 71.0 / 219.4 | 63.3 / 273.0 | 61.4 / 204.2 | 64 | 64 | 36 |

**Use**: these runs are the reference for attributing costs at CP1–CP3. The final CP0-versus-CP4 comparison uses fresh, interleaved CP0 runs from a worktree of the baseline commit (T152).

## CP1: core checkout path (T075)

**Measured**: 2026-10-21, 17:04–17:07 local time, two back-to-back runs on the benchmark machine.

**Implementation**: the Phase 4 (US1) tree, working copy on top of commit `64e033d`, so `evidence.json` records `gitDirty=true` with the JAR SHA-256s of the measured build. Scan is memory-first (US2), and Complete is the Saga: T1, then one batched `ApplyCheckout`, then T3. Analytics and the outbox relay are not wired yet.

**Command**: from the repository root, with the three service passwords in the environment:

```powershell
./server/benchmark-microservices.ps1 -Workload default -SkipAnalytics -OutputLabel cp1-default-1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe>
```

The second run uses `-OutputLabel cp1-default-2`.

**Caveat**: both runs skip Analytics, so the harness marks them `comparable=false`. The comparison with CP0 only attributes costs; it is not a result. CP0 ran all three services and its Analytics materializer on the shared database.

**Validation**: `db/validate-invariants.ps1` passed both runs, for Transaction and Inventory. Every transaction completed; there were zero Complete errors, background completions, or completions without a recorded response, and the remaining difference was 0 (FR-054, SC-010). Stock dropped by exactly the COMPLETED quantities. The accepted scans stay undelivered until US5 wires the relay.

**Server-side metrics**: the final `PERF_METRICS` summary of each service, from `evidence.json`. The histogram buckets are coarse (…, 12.8, 25.6, 51.2, 102.4 ms), so p99 values are bucket bounds.

| Metric | `cp1-default-1` | `cp1-default-2` |
|---|---:|---:|
| Scan, `transaction.http.scan` p95 / p99 (ms) | 12.8 / 27.9 | 12.8 / 30.4 |
| Complete, `transaction.http.complete` p95 / p99 (ms) | 51.2 / 102.4 | 60.9 / 102.4 |
| Transaction pool borrow wait / hold time p99 (ms) | 0.05 / 27.9 | 0.05 / 30.4 |
| Inventory pool borrow wait / hold time p99 (ms) | 0.05 / 43.1 | 0.05 / 55.8 |
| Peak pool occupancy, Transaction / Inventory | 10 / 6 | 10 / 6 |
| `transaction.lock.wait` p99 (ms) | 0.05 | 0.05 |
| `transaction.lock.contended` / `transaction.lock.collisions` | 92 / 92 | 90 / 89 |
| `inventory.apply.lockWait` p99 (ms) | 23.5 | 25.6 |
| `transaction.rpc.applyCheckout` p95 / p99 (ms) | 25.6 / 43.1 | 27.9 / 55.8 |
| Commit p99: T1 / Inventory apply / T3 / Scan (ms) | 23.5 / 21.5 / 19.7 / 23.5 | 25.6 / 23.5 / 23.5 / 25.6 |
| `Com_commit` Δ ÷ `totalTransactions` | 14.44 | 14.38 |
| Scans per basket | 10.44 | 10.38 |
| Background completions | 0 | 0 |
| In-request `ApplyCheckout` retries, budget expiries, ambiguous commits, guard misses | 0, 0, 0, 0 | 0, 0, 0, 0 |

**Attribution** (R18): CP1 is 4.3% and 5.0% below the CP0 default median. The second run crosses the 5% line, so T075 requires the cost attribution before US3. It is recorded here from the client means, which add up per basket, and the commit counts.

| Per basket, client mean (ms) | CP0 (`cp0-default-3`, the median) | CP1 (mean of both runs) | Change |
|---|---:|---:|---:|
| Start | 8.61 | 7.65 | −0.96 |
| Scans: count × mean | 10.55 × 9.65 = 101.7 | 10.41 × 8.96 = 93.3 | −8.4 |
| Complete | 15.90 | 31.39 | +15.49 |
| Total | 126.2 | 132.3 | +6.1 (+4.8%) |

- **Mandatory commits**: commits per completed transaction are exactly N + 4 (Start, N Scans, T1, Inventory's apply, T3), against N + 2 in the single-database design. The two extra commits are required: Inventory owns its stock and commits it (C-II), and T1 must be durable before the remote step (C-V). At this machine's cost of about 7.5 ms per durable commit (R18), they account for the whole +15.5 ms on Complete: the mean rose from one commit (15.9 ms) to three serialized commits plus one RPC (31.4 ms).
- **Optional cost is already at its design budget**: T1, T3, and the Inventory apply run 2, 2, and 3 statements with no read, and the receipt needs no read (T065, T066). No in-request retry, guard miss, or ambiguous commit occurred.
- **Round trips saved**: the memory-first Scan and Start save 0.69 ms per Scan and 0.96 ms per Start at the mean, 9.4 ms per basket in total. `Innodb_row_lock_waits` Δ fell from 2,197–2,267 to 1,518–1,533 (−31%).
- **Net**: the basket takes 6.1 ms (4.8%) longer, which matches the 4.3–5.0% drop in `totalTransactions` under the Load Client's fixed stations. The drop is the price of the two mandatory commits, not an optional cost. CP2 adds Analytics and the relay; CP3 measures tuning variants; only the interleaved CP0-versus-CP4 runs (T152) compare the designs.
- **Hot SKUs**: Inventory's locking SELECT waits up to 23.5–25.6 ms at p99. This contention on the Load Client's skewed SKUs is the largest part of `ApplyCheckout`'s tail and is the entry evidence for the deferred split-hot-SKU option (R23); nothing is changed at CP1.

## CP2: full system (T104)

**Measured**: 2026-10-21, 18:47–18:53 local time: `cp2-default-1`, `cp2-default-2`, then `cp2-stress-1`, back to back on the benchmark machine.

**Implementation**: the Phase 6 (US5) tree, working copy on top of commit `c3ee3cc`, so `evidence.json` records `gitDirty=true` with the JAR SHA-256s of the measured build. All three services run: Scan writes the outbox row, the relay delivers it with `DeliverScanEvents`, and Analytics ingests with positions and publishes windows through the unchanged pipeline.

**Command**: from the repository root, with the three service passwords in the environment:

```powershell
./server/benchmark-microservices.ps1 -Workload default -OutputLabel cp2-default-1 -MySqlExe <mysql.exe> -NginxBin <nginx.exe> -JavaExe <JDK21-java.exe>
```

The second default run uses `-OutputLabel cp2-default-2`; the stress run uses `-Workload stress -OutputLabel cp2-stress-1`.

**Validation**: all three runs are `comparable=true`. `db/validate-invariants.ps1` passed for Transaction, Inventory, and Analytics. Analytics stored exactly one position per accepted scan (46,492, 45,568, and 185,512), with 91, 90, and 370 published windows. Every transaction completed, with zero background completions or completions without a recorded response, and the remaining difference was 0 (FR-054). The relay saw no failures, and Analytics counted no duplicates. Every stress Complete error is the business rejection `409 INSUFFICIENT_STOCK`: Inventory answered all 17,628 `ApplyCheckout` calls with OK (11,185 applied, 6,443 rejected), and 8 per-call deadline expiries were retried within the request.

**Server-side metrics**: the final `PERF_METRICS` summary of each service, from `evidence.json`. The histogram buckets are coarse, so p99 values are bucket bounds.

| Metric | `cp2-default-1` | `cp2-default-2` | `cp2-stress-1` |
|---|---:|---:|---:|
| `transaction.lock.wait` p99 (ms) | 0.05 | 0.05 | 0.05 |
| `transaction.lock.contended` / `transaction.lock.collisions` | 76 / 75 | 167 / 167 | 950 / 950 |
| Pool borrow wait p99 (ms): Transaction / Inventory / Analytics | 0.05 / 0.05 / 0.05 | 0.05 / 0.05 / 0.05 | 0.05 / 893.3 / 0.05 |
| Pool hold time p99 (ms): Transaction / Inventory / Analytics | 33.2 / 55.8 / 33.2 | 39.5 / 55.8 / 39.5 | 55.8 / 751.2 / 51.2 |
| Peak pool occupancy: Transaction / Inventory / Analytics | 12 / 6 / 3 | 12 / 5 / 2 | 64 / 48 / 3 |
| `inventory.apply.lockWait` p99 (ms) | 27.9 | 25.6 | 751.2 |
| Relay backlog peak (undelivered rows, 2 s samples) | 32 | 44 | 126 |
| Unpublished-boundary peak | 1 | 1 | 1 |
| Relay: rounds / events per round | 3,224 / 14.4 | 3,154 / 14.4 | 5,218 / 35.6 |
| `transaction.relay.latency` p99, commit to acknowledgement (ms) | 93.9 | 111.7 | 172.2 |
| `analytics.window.publishLag` p99, ingestion to publication (ms) | 144.0 | 125.0 | 132.8 |
| Visibility delay p99, upper bound (sum of the two above, ms) | 238 | 237 | 305 |
| Catch-up after load stops | first drain sample (≤ 2 s) | first drain sample (≤ 2 s) | first drain sample (≤ 2 s) |
| Background completions | 0 | 0 | 0 |
| `transaction.recovery.scanQueries` / `transaction.wait.dbChecks` | 3 / 0 | 3 / 0 | 5 / 0 |
| Idle polls per second of run time: relay / Analytics poller | 5.4 / 12.3 | 5.4 / 12.3 | 2.7 / 10.8 |
| Scan: server mean, `transaction.http.scan` (ms) / Scan commit mean (ms) | 8.80 / 8.09 | 8.97 / 8.17 | 10.60 / 9.46 |
| `Com_commit` Δ ÷ `totalTransactions` | 16.17 | 16.05 | 23.86 |

- **Visibility**: no event carries its acceptance time into Analytics' tables, so visibility delay is bounded by the relay's commit-to-acknowledgement latency plus the window's ingestion-to-publication lag. Both p99 bounds stay under 0.31 s, far inside the 5 s target, and every run drained by the first post-load sample, far inside the 60 s ceiling (SC-007).
- **Idle polls**: the counters cover each service's whole lifetime, divided here by the client's run time. Each idle poll is one indexed read with a mean of about 0.4 ms.

**Investigation** (Implementation Rule 6; `totalTransactions` fell more than 5% below CP0):

| Per basket, client mean (ms) | CP0 (`cp0-default-3`, the median) | CP1 (mean of both runs) | CP2 (mean of both runs) | CP2 − CP1 |
|---|---:|---:|---:|---:|
| Start | 8.61 | 7.65 | 8.01 | +0.36 |
| Scans: count × mean | 10.55 × 9.65 = 101.7 | 10.41 × 8.96 = 93.3 | 10.62 × 9.28 = 98.5 | +5.2 |
| Complete | 15.90 | 31.39 | 32.17 | +0.78 |
| Total | 126.2 | 132.3 | 138.7 | +6.4 (+4.8%) |

- **Default, −8.5% and −9.4% against CP0**: CP1's mandatory Saga commits explain about 4.7% (T075). The remaining −4.0% and −4.9% against CP1 match the +4.8% per-basket time. About 1.9 ms of the +5.2 ms on Scans comes from longer random baskets (10.62 against 10.41 scans), which is workload variance, not cost. The rest is a uniform slowdown: Start +0.36 ms, each Scan +0.32 ms (server mean +0.24 to +0.41 ms, Scan commit +0.18 to +0.26 ms), and Complete +0.78 ms.
- **Cause: the relay's commits, not its reads**: commits per basket rose by 1.7 to N + 5.5. The relay delivers small batches, a mean of 14.4 events per round at 10 stations, about 54 rounds per second. Each round adds one durable `delivered_at` commit in Transaction and one ingestion commit in Analytics, both on the same MySQL instance with `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`. That is about 1.5 extra durable commits per basket, which compete with Start, Scan, and Complete for the redo and binary-log flush. Idle polls are only 5 and 12 per second at about 0.4 ms each, so they are not the cost.
- **R23 evidence**: relay writes measurably slow Scans. This meets the entry criteria of two deferred options: "coalescing relay rounds after partial batches" and "in-memory relay queue or watermark". Coalescing is the cheaper candidate. It keeps per-row `delivered_at` and at-least-once delivery, and the 5 s visibility target leaves more than 4.6 s of headroom. Per R23, it must be confirmed by a CP3 A/B before adoption, so nothing is changed at CP2.
- **Stress, −22.2% against CP0**: dominated by Inventory's hot-SKU row locks. `inventory.apply.lockWait` p99 is 751 ms, and the Inventory pool sits at its 48-connection ceiling with a 893 ms p99 borrow wait, so Complete's mean is 480 ms. The relay's share of the stress run cannot be separated, because CP1 ran no stress workload. This is the entry evidence for the deferred split-hot-SKU option, but that criterion also requires an unaffected default workload, which CP2's relay cost prevents. CP3 should first separate the relay's cost.
- Statement counts and latencies explain these results; they establish no improvement. Only the interleaved CP0-versus-CP4 runs (T152) compare the designs.

## CP3: tuning A/B (T123)

**Measured**: 2026-10-21 21:16 to 2026-10-22 20:27 local time, with pauses. Every comparison is an interleaved A/B run from `server/benchmark-compare.ps1`. Variant A is the current defaults, and variant B differs only in the knob under test. All variants use the same build, working copy on top of commit `f51e85d`: `gitDirty=true`, with the JAR SHA-256s in each `evidence.json`. Every run had its own full reset, all three services, and the exact Load Client command, and passed the validator (`comparable=true`).

**Method**: the comparisons for server prepared statements and the 10/32 Inventory pool ran under the original R22 rule, which extended close results to five pairs and decided them with a one-sided Mann–Whitney U test. The driver is `compare/cp3-campaign-v1.ps1`. On 2026-10-22 the policy was streamlined (R22, T123), and the driver became `compare/cp3-campaign.ps1`:
- differences within ±3% are practically equivalent;
- a screen is one pair per relevant workload, and pool sizes are measured only under stress;
- three fresh confirmation pairs are run only after a screen of +5% or more;
- the cache-off attribution run was skipped.

None of the four earlier comparisons screened at +5%, so both earlier variants are rejected under either rule.

**Confidence**: a one-pair screen cannot separate differences under about 3–5% from run-to-run noise. The incumbent's own `totalTransactions` moved between screens: 4,102–4,381 at default and 10,416–11,131 under stress. Only the paired difference inside one comparison is meaningful. No result below claims statistical significance.

| Variant (B) | Workload | Pairs | Defaults (A) median (range) | Variant (B) median (range) | Δ B vs A | A vs CP0 | B vs CP0 | Outcome |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `DB_SERVER_PREPARED_STATEMENTS=true` | default | 1 (screen) | 4174 | 4328 | +3.69% | −12.3% | −9.1% | screened on |
| | stress | 1 (screen) | 10982 | 10946 | −0.33% | −23.6% | −23.9% | screened on |
| | default | 5 | 4344 (4161–4390) | 4402 (4318–4451) | +1.34% | −8.7% | −7.5% | no demonstrated improvement |
| | stress | 5 | 11131 (10955–11398) | 11217 (11048–11317) | +0.77% | −22.6% | −22.0% | no demonstrated improvement |
| Inventory pool 10/32 | default | 1 (screen) | 4381 | 4346 | −0.80% | −7.9% | −8.7% | screened on |
| | stress | 1 (screen) | 10796 | 11048 | +2.33% | −24.9% | −23.1% | screened on |
| | default | 5 | 4201 (3841–4285) | 4201 (4068–4395) | 0.00% | −11.7% | −11.7% | no demonstrated improvement |
| | stress | 5 | 11000 (10486–11183) | 10945 (10920–11280) | −0.50% | −23.5% | −23.9% | no demonstrated improvement; Complete p99 +34% |
| Inventory pool 24/64 | stress | 1 (screen) | 10416 | 10789 | +3.58% | −27.5% | −24.9% | no demonstrated improvement |
| Transaction pool 48/80 | stress | 1 (screen) | 10641 | 10632 | −0.08% | −26.0% | −26.0% | no demonstrated improvement |
| `OUTBOX_COALESCE_MS=100` | default | 1 (screen) | 4102 | 4135 | +0.80% | −13.8% | −13.1% | no demonstrated improvement |
| | stress | 1 (screen) | 10548 | 10786 | +2.26% | −26.6% | −25.0% | no demonstrated improvement |

CP0 medians: default 4,759 and stress 14,375.5 (T001). The labels are `cp3-<variant>-<workload>` and `cp3-screen-<variant>-<workload>`. Each comparison's `compare/<label>/compare.json` holds every run, its background CPU, and every invalidated attempt.

**Decision**: no variant is kept. Every default stays as it was:
- `DB_SERVER_PREPARED_STATEMENTS=false`;
- Transaction pool 32/64, Inventory pool 16/48, Analytics pool 10;
- `OUTBOX_COALESCE_MS=0`, which leaves relay coalescing off;
- `TRANSACTION_CACHE_MAX_ENTRIES=100000`.

T124 therefore changes no default and no statement budget.

**Attribution** (explanations only, Implementation Rule 6):
- **Pools**: neither pool size changed the stress result beyond noise. Stress is bound by Inventory's row locks on the hot SKUs (`inventory.apply.lockWait` p99 of 0.53–1.26 s across the stress runs), not by pool queuing. A smaller Inventory pool cut that lock wait p99 from 893 to 689 ms over five pairs. It also raised Complete p99 from 1.49 to 1.99 s and left `totalTransactions` unchanged, which is the expected trade of waiting for a connection instead of a row lock. The split-hot-SKU option (R23) remains the only lever on this limit, and its default-workload entry criterion is still unmet.
- **Relay coalescing**: it did what CP2 predicted mechanically, cutting commits per completed transaction from 15.99 to 14.67 at default and from 23.46 to 22.82 under stress. It gained only +0.8% and +2.3% in `totalTransactions`, inside the noise of a single pair. The relay's extra commits cost less than CP2's per-basket estimate suggested, so coalescing is not adopted. The switch stays in the code, off by default, as a documented A/B setting like `DB_SERVER_PREPARED_STATEMENTS`.
- **Server prepared statements**: commits per completed transaction were unchanged (16.06 against 15.96 at default, 24.22 against 24.48 under stress). A likely reason, not measured here, is that Connector/J's client-side statement cache (R20) already removes most of the per-statement parse cost.

**Harness and machine notes**:
- **First attempt** (2026-10-21 21:20): `benchmark-compare.ps1` failed with "Argument types do not match" before recording its first one-pair screen. Windows PowerShell 5.1 throws on `@($list).Count` for a one-element generic List. The fix counts runs through the pipeline. The pair is kept, unused, in `compare/aborted-attempts/`.
- **Machine load** (2026-10-21 21:50–22:30): a game (`WardogsClient-Win64-Shipping`) used about 12 cores. Defaults dropped about 10%, and two default runs had 20 s commit stalls. Every run that overlapped it is kept, unused, in `compare/invalidated-machine-load/`. An idle-machine gate was then added. Before each run it waits for named blocking processes and for low background CPU. Afterwards it measures that run's background CPU and repeats a loaded run under the same label. The limit was 1.0 of the 24 logical cores, raised on 2026-10-22 at 19:54 to 2.0, because ordinary desktop load of about 1.1–1.5 cores invalidated every attempt. The game is blocked by name, as is a second game seen later (`ROTTR`).
- **Port conflicts** (2026-10-22 17:28): this machine's TCP dynamic port range starts at 1024, so another program (`WeChatAppEx`) took gRPC port 9093 as a client port, and Analytics could not bind it. The three affected screens are in `compare/failed-port-conflict/`. A logged "REJECT inv10-32" was caused by that failure, not measured, and the variant was screened again. The gate now waits until no other program holds a benchmark port, and it repeats a run whose services fail to start.
- **Interruptions**: one shutdown and two ended sessions interrupted the campaign. `benchmark-compare.ps1` now resumes an interrupted comparison from its complete pairs; `cp3-ps-stress` resumed with two. An orphaned driver from an ended session briefly ran beside a new one, and both were stopped before either measured anything.
- **Superseded runs**: the half-finished Inventory 24/64 default screen is in `compare/superseded-by-streamlined-policy/`, because the streamlined policy measures pool sizes only under stress.

## CP4: final CP0-versus-CP4 comparison (T152)

**Measured**: 2026-10-22, 21:01–21:41 local time: three default pairs, then three stress pairs, interleaved on the same machine and evening, under the idle-machine gate (2.0 cores). No run was invalidated or resumed.

**Variants**:
- **A, CP0**: the service-based implementation at commit `26c89af`, from the worktree `..\cs6510-cp0`, built with its own `server/build.sh` and `load-client/build.sh`. Each run was its own T001 harness run: `benchmark-scan.ps1 -Variant retained -Workload <workload> -TransactionPoolSize 32 -TransactionPoolMaxSize 64`. The harness ran in its own `powershell.exe` and received the one-line T001 harness fix, which changes no measured code.
- **B, CP4**: the microservices build that passed T125, working copy on top of `f51e85d`, with every default unchanged by CP3.

**Official reports**: `cp4-final-default-b-1` and `cp4-final-stress-b-1`, designated before the runs and validated by T126 and T127. Both were the first attempt of their run.

**Confidence**: three pairs per workload describe the change; they do not test it, and no statistical significance is claimed. CP0 was ahead in every pair of both workloads.

| Workload | Pair | CP0 `totalTransactions` | CP4 `totalTransactions` | Δ |
|---|---:|---:|---:|---:|
| default | 1 | 4978 | 4326 | −13.10% |
| default | 2 | 4861 | 4261 | −12.34% |
| default | 3 | 4879 | 4339 | −11.07% |
| stress | 1 | 14296 | 11105 | −22.32% |
| stress | 2 | 14916 | 10413 | −30.19% |
| stress | 3 | 14918 | 11240 | −24.65% |

| Workload | Variant | `totalTransactions` median (range) | Completed/s median | Start p95 / p99 | Scan p95 / p99 | Complete p95 / p99 | Errors (all operations) |
|---|---|---:|---:|---:|---:|---:|---|
| default | CP0 | 4879 (4861–4978) | 81.12 | 12.0 / 28.8 | 13.4 / 32.3 | 32.2 / 54.3 | 0 |
| default | CP4 | 4326 (4261–4339) | 71.76 | 12.9 / 29.2 | 13.8 / 35.4 | 59.8 / 110.6 | 0 |
| stress | CP0 | 14916 (14296–14918) | 123.77 | 60.9 / 103.8 | 59.1 / 98.6 | 692.5 / 797.8 | Complete 41,202 of 85,332 (48.3%), all `409 INSUFFICIENT_STOCK` |
| stress | CP4 | 11105 (10413–11240) | 91.95 | 21.0 / 55.7 | 23.2 / 56.2 | 1023.4 / 1458.2 | Complete 18,876 of 51,634 (36.6%), all `409 INSUFFICIENT_STOCK` |

Latencies are client-side medians over each variant's three runs, in milliseconds.

**Result**: CP4 completes **11.3% fewer** transactions than CP0 at default (median 4,326 against 4,879) and **25.6% fewer** under stress (11,105 against 14,916). This is the only comparison of the two designs (Implementation Rule 6). The microservices design does not improve `totalTransactions` on this machine.

**Why** (attribution from CP1–CP3, explanations only):
- **Default**: CP4 commits N + 4 durable transactions per basket for N Scans: Start, the Scans, T1, Inventory's apply, and T3. CP0 commits N + 2. Database per service (C-II) and a durable T1 before the remote step (C-V) make those two extra commits mandatory. They serialize on Complete, whose p95 roughly doubles from 32 to 60 ms. The relay and Analytics' ingestion add about 1.5 durable commits per basket on the same MySQL instance (CP2). The official default run measured 15.81 commits per completed transaction at 10.36 Scans per basket, which is N + 5.45. The memory-first Scan and Start kept their latency level with CP0 despite those commits.
- **Stress**: Inventory's row locks on the hot SKUs dominate. `inventory.apply.lockWait` p99 was 689 ms, and the Inventory pool ran at its 48-connection ceiling with a 974 ms p99 borrow wait. Complete p95 rose from 0.69 to 1.02 s, and each station starts fewer baskets. CP4's Start and Scan are 2–3 times faster than CP0's under stress (Scan p99 56 against 99 ms), because they no longer queue behind Complete for the Transaction pool. But a basket still waits on the hot-SKU lock at Complete. CP3 found no pool size or switch that changes this; the split-hot-SKU option (R23) is the remaining lever.

**Durability and completion (FR-054)**: in both official runs every COMPLETED transaction matches a client success, with zero background completions, zero completions without a recorded response, and a remaining difference of 0. `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and `log_bin=1` were unchanged in every run.

**Harness notes**:
- The first attempt at `cp4-final-default` failed at 21:00 before any load. `benchmark-compare.ps1` called the CP0 harness in-process with redirected streams, and Windows PowerShell 5.1 turned Nginx's ordinary stderr into a terminating error under the harness's own `Stop` preference. Its only output, a `samples.csv`, is kept in `compare/cp4-final-default/failed-harness/`. The script now runs the CP0 harness as a separate `powershell.exe`, as T001 ran it, and the comparison was rerun from the start.
- The CP0 harness samples a commit counter that does not change during its runs, so CP0's commits per basket are given by its design (N + 2), not measured.
