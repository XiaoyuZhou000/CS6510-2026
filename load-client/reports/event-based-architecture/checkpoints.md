# Benchmark Checkpoints: Event-Based Architecture

Checkpoint record for `specs/006-event-based-architecture` (plan *Performance Strategy*, research D15). Every row reports the client-observed `totalTransactions` and its change against the CP0 median of the same workload. Fewer statements or lower latency never count as an improvement on their own.

- **CP0–CP3** attribute costs during implementation.
- **Final comparison**: only the fresh, interleaved CP0 and CP4 runs (T166) compare the designs.

## Results

Latencies are client-side p95 / p99 in milliseconds, from the Load Client JSON report. *Background completions* counts attempts that settled after their Complete request had already answered. *Parked events* and *final consumer lag* are blank for CP0, which has no broker.

| Checkpoint | Label | Workload | `totalTransactions` | Δ vs CP0 median (%) | Start p95 / p99 | Scan p95 / p99 | Complete p95 / p99 | Complete errors | Background completions | Parked events | Final consumer lag | `innodb_flush_log_at_trx_commit` | `sync_binlog` | Harness output |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| CP0 | `cp0-default-1` | default | 4736 | −1.7 | 11.5 / 26.0 | 13.0 / 33.5 | 56.3 / 117.4 | 0 | 0 | | | 1 | 1 | [cp0-default-1](cp0/cp0-default-1/) |
| CP0 | `cp0-default-2` | default | 4897 | +1.7 | 11.3 / 26.6 | 12.8 / 34.6 | 51.1 / 107.6 | 0 | 0 | | | 1 | 1 | [cp0-default-2](cp0/cp0-default-2/) |
| CP0 | `cp0-stress-1` | stress | 11427 | 0.0 | 18.6 / 48.3 | 20.7 / 52.3 | 944.0 / 1286.0 | 7321 | 0 | | | 1 | 1 | [cp0-stress-1](cp0/cp0-stress-1/) |

## Summary

| Checkpoint | Workload | Runs | `totalTransactions` median | Range (min–max) |
|---|---|---:|---:|---|
| CP0 | default | 2 | 4816.5 | 4736–4897 |
| CP0 | stress | 1 | 11427 | 11427 |

## CP0: attribution baseline (T002)

**Measured**: 2026-10-29, 18:00–18:07 local time, three back-to-back runs on the benchmark machine.

**Implementation**: the unmodified feature 005 microservices implementation at commit `8891c57`, built with `server/build.sh` and `load-client/build.sh` before any code change of this feature. The working tree differed from that commit only in documentation: the uncommitted constitution v6.0.0 and the untracked `specs/006-event-based-architecture/`, so `evidence.json` reports `gitDirty: true`. No source, script, or configuration file was changed.

**Command**: from the repository root, with the three `*_DB_PASSWORD` user environment variables loaded:

```powershell
server/benchmark-microservices.ps1 -Workload default -OutputLabel cp0-default-1 -NginxBin <nginx.exe>
```

The second default run uses the label `cp0-default-2`, and the stress run uses `-Workload stress -OutputLabel cp0-stress-1`.

**Shared-machine gate**: the feature 005 harness has no idle gate, so a wrapper waited before each run until neither `WardogsClient-Win64-Shipping` nor `ROTTR` was running and total CPU use stayed at or below 2.0 cores for three consecutive 10 s samples, the same thresholds as `server/benchmark-compare.ps1 -IdleCpuCores 2.0`. No run failed or needed a retry.

**Environment**: Windows 10, JDK 21, MySQL 8.4.4 on `127.0.0.1:3307`, and Nginx 1.31.6. Docker Desktop was not running. `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1` for every run. No server variable was changed.

**Harness notes**:
- **Output location**: `benchmark-microservices.ps1` writes labelled runs to `load-client/reports/microservices-architecture/<label>/`. The three CP0 directories were moved unchanged to `cp0/<label>/` here, so this feature's evidence stays in one folder.
- **Use**: these runs are the attribution reference for CP1–CP3. The CP0-versus-CP4 comparison uses fresh interleaved runs (T166).

**Validation**: `db/validate-invariants.ps1` passed and all pending work drained for every run, so every run is `comparable: true`.
