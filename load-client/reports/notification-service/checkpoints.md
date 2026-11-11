# Benchmark Checkpoints: Notification Service

Checkpoint record for `specs/007-notification-service` (plan *Performance Strategy*, research N15). Every row reports the client-observed `totalTransactions` and its change against the CP0 median of the same workload. Fewer statements or lower latency never count as an improvement on their own. A drop of more than 5% between consecutive checkpoints is investigated before work continues.

- **CP0**: feature 006 unmodified, before any code change of this feature (T002).
- **CP1**: `SaleCompleted` in T3, with Notification not running (end of US1).
- **CP2**: the full system with 100 subscribers.
- **CP3**: optional A/B of consumer threads, lane in-flight cap, and marking interval.
- **CP4**: the official runs.

## Results

Latencies are client-side p95 / p99 in milliseconds, from the Load Client JSON report. *Background completions* counts attempts that settled after their Complete request had already answered.

| Checkpoint | Label | Workload | `totalTransactions` | Δ vs CP0 median (%) | Start p95 / p99 | Scan p95 / p99 | Complete p95 / p99 | Complete errors | Background completions | Parked events | Final consumer lag | `innodb_flush_log_at_trx_commit` | `sync_binlog` | Harness output |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| CP0 | `cp0-default-1` | default | 4454 | +2.6 | 12.6 / 31.2 | 13.7 / 42.9 | 69.1 / 142.6 | 0 | 0 | 0 | 0 | 1 | 1 | [cp0-default-1](cp0/cp0-default-1/) |
| CP0 | `cp0-default-2` | default | 4228 | −2.6 | 12.8 / 26.6 | 14.8 / 46.4 | 71.6 / 134.9 | 0 | 0 | 0 | 0 | 1 | 1 | [cp0-default-2](cp0/cp0-default-2/) |
| CP0 | `cp0-stress-1` | stress | 10494 | 0.0 | 27.2 / 63.4 | 29.3 / 75.9 | 1675.9 / 2248.5 | 5061 | 0 | 0 | 0 | 1 | 1 | [cp0-stress-1](cp0/cp0-stress-1/) |

CP0 medians: default 4341, stress 10494.

## CP0 (T002)

**Date**: 2026-11-11.

**Code**: feature 006 unmodified. `evidence.json` records commit `5986398` with a clean tree (`gitDirty: false`). That commit differs from `4a91686` only in `.specify/memory/constitution.md` and `specs/007-notification-service/`, so every service, client, database, and deployment file is the one at `4a91686`. All three runs used the same jars (the `jarSha256` block of each `evidence.json`), built with `server/build.sh` and `load-client/build.sh` immediately before the first run.

**Command**: `server/benchmark-event-based.ps1 -Workload default|stress -OutputLabel <label> -IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`, run as `cp0-default-1`, `cp0-default-2`, and `cp0-stress-1` in that order. The unchanged harness writes under `load-client/reports/event-based-architecture/<label>/`. Each run folder was then moved unchanged into `cp0/` here, so feature 006's evidence tree is untouched.

**Environment**:
- Kafka 4.1.0 (`kafka_2.13-4.1.0.jar`), native, with page-cache flushing (the 006 default). It was reinstalled on 2026-11-11 under `C:\Users\18422\cs6510-tools\kafka_2.13-4.1.0` from the official tarball, SHA-512 verified, because a Windows temp cleanup had emptied the previous `%TEMP%` install. This is the same release and the same `deploy/kafka/server.properties` as in feature 006.
- Nginx 1.31.6 from `C:\Users\18422\cs6510-tools\nginx-1.31.6`.
- MySQL 8.4 with `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, and `log_bin=1`.

**Validation**: every run is `comparable: true`:
- the validator passed for all three services;
- all work drained, with zero parked events, zero final consumer lag, zero background completions, and zero completions without a recorded response;
- the broker stayed up;
- the idle gate passed, with background CPU of 0.41, 0.93, and 1.04 cores and no blocking process seen.

**Observations**:
- The two default runs are 5.3% apart (4454 vs. 4228). Each is within ±3% of their median, the equivalence band used since feature 005.
- All 5061 Complete errors of the stress run are 409 `INSUFFICIENT_STOCK` once the hot SKU sells out (every line of `cp0-stress-1/client-stderr.txt`). Start and Scan had zero errors.
- These figures are consistent with feature 006's official CP4 runs on this machine (default 4371, stress 10553 with 5954 Complete errors).
