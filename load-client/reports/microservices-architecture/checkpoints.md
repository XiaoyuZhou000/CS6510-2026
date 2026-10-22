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

## Summary

| Checkpoint | Workload | Runs | `totalTransactions` median | Range (min–max) |
|---|---|---:|---:|---|
| CP0 | default | 3 | 4759 | 4743–4850 |
| CP0 | stress | 2 | 14375.5 | 14330–14421 |
| CP1 | default, without Analytics | 2 | 4537 | 4519–4555 |

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
