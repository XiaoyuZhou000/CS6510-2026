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

## Summary

| Checkpoint | Workload | Runs | `totalTransactions` median | Range (min–max) |
|---|---|---:|---:|---|
| CP0 | default | 3 | 4759 | 4743–4850 |
| CP0 | stress | 2 | 14375.5 | 14330–14421 |

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
