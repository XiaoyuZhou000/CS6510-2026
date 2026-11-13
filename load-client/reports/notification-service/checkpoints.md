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

| CP1 | `cp1-default-1` | default | 4294 | −1.1 | 12.0 / 26.3 | 13.7 / 47.1 | 70.7 / 144.4 | 0 | 0 | 0 | 0 | 1 | 1 | [cp1-default-1](cp1/cp1-default-1/) |
| CP1 | `cp1-default-2` | default | 4398 | +1.3 | 12.3 / 29.5 | 13.3 / 44.0 | 71.5 / 138.8 | 0 | 0 | 0 | 0 | 1 | 1 | [cp1-default-2](cp1/cp1-default-2/) |
| CP1 | `cp1-stress-1` | stress | 10787 | +2.8 | 24.2 / 61.2 | 26.1 / 67.2 | 1589.2 / 2179.7 | 5760 | 0 | 0 | 0 | 1 | 1 | [cp1-stress-1](cp1/cp1-stress-1/) |

CP0 medians: default 4341, stress 10494. CP1 medians: default 4346 (+0.1%), stress 10787 (+2.8%).

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

## CP1 (T035)

**Date**: 2026-11-12.

**Code**: commit `b2e48fc`, with a clean tree (`gitDirty: false` in every `evidence.json`). That commit is the end of US1, plus US2 and US3. Transaction records a `SaleCompleted` row in T3 and relays it to `selfcheckout.sale-completed.v1`. Notification was not running: the 006 harness starts only Inventory, Transaction, and Analytics. All three runs used the same jars, built with `server/build.sh` on 2026-11-11 23:44.

**Command**: the CP0 command, `server/benchmark-event-based.ps1 -Workload default|stress -OutputLabel <label> -IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights` (plus `-NginxBin`). The runs went in the order `cp1-default-1`, `cp1-default-2`, `cp1-stress-1`. The harness labels were `n007-cp1-*`, because `load-client/reports/event-based-architecture/cp1-*` already holds feature 006's own CP1 runs. Each folder was then moved unchanged into `cp1/` here.

**Environment**: as for CP0. The broker was restarted with `deploy/kafka/start-broker.ps1` earlier that day, after a machine restart.

**Validation**: every run is `comparable: true`:
- the validator passed for all three services;
- all work drained, with zero parked events, zero final consumer lag, zero background completions, and zero completions without a recorded response;
- the broker stayed up;
- the idle gate passed, with background CPU of 0.79, 0.28, and 0.24 cores. Before the second and third runs, the gate waited about 80 s for the previous run's ports to close.

The sale-completed topic ended with exactly one record per completed sale (stress: 10,787 records for 10,787 transactions).

**Comparison with CP0**: no regression. The default median is +0.1% (4346 vs. 4341), and both default runs are within ±1.3% of the CP0 median. The stress run is +2.8% (10787 vs. 10494). The design estimated a cost of at most 3% (research N15); the measured cost is within run-to-run noise. Latencies are the same within noise: Complete p95 is 70.7 / 71.5 ms vs. CP0's 69.1 / 71.6 ms. All 5760 Complete errors of the stress run are 409 `INSUFFICIENT_STOCK`, as in CP0 (5061).
