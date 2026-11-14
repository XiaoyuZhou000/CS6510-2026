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

| CP2 | `cp2/default-1` | default | 3840 | −11.5 | 15.2 / 46.7 | 17.1 / 53.9 | 100.2 / 175.1 | 0 | 0 | 0 | 0 | 1 | 1 | [default-1](cp2/default-1/) |
| CP2 | `cp2/default-2` | default | 3643 | −16.1 | 14.9 / 45.7 | 17.2 / 58.4 | 102.8 / 181.1 | 0 | 0 | 0 | 0 | 1 | 1 | [default-2](cp2/default-2/) |
| CP2 | `cp2/stress-1` | stress | 9784 | −6.8 | 38.0 / 82.7 | 40.8 / 87.7 | 1915.1 / 2582.0 | 3669 | 0 | 0 | 0 | 1 | 1 | [stress-1](cp2/stress-1/) |
| CP2 control (Notification off) | `cp2/control-1` | default | 4152 | −4.4 | 13.0 / 34.4 | 14.6 / 47.2 | 74.8 / 140.0 | 0 | 0 | 0 | 0 | 1 | 1 | [control-1](cp2/control-1/) |
| CP2 control (Notification off) | `cp2/control-2` | default | 3926 | −9.6 | 12.4 / 30.2 | 14.3 / 43.9 | 73.4 / 142.9 | 0 | 0 | 0 | 0 | 1 | 1 | [control-2](cp2/control-2/) |

| CP4 | `cp4/default` | default | 3523 | −18.8 | 16.1 / 51.6 | 17.3 / 59.4 | 110.9 / 187.3 | 0 | 0 | 0 | 0 | 1 | 1 | [default](cp4/default/) |
| CP4 | `cp4/stress` | stress | 9210 | −12.2 | 44.0 / 85.5 | 47.4 / 100.9 | 2064.0 / 2784.8 | 2775 | 0 | 0 | 0 | 1 | 1 | [stress](cp4/stress/) |

CP0 medians: default 4341, stress 10494. CP1 medians: default 4346 (+0.1%), stress 10787 (+2.8%). CP2 medians: default 3742 (−13.8%), stress 9784 (−6.8%); the same day's control without Notification: default 4039 (−7.0%).

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

## CP2 (T122)

**Date**: 2026-11-13.

**Code**: commit `b518303` (phase 9) and the jars built from it. `run.json` reports `gitDirty: true` for these runs, because phase 10's test and report sources were being edited while they ran. No service jar, harness, or Load Client file changed during the runs.

**Command**: `load-client/reports/notification-service/run-evidence.ps1`, which runs `server/benchmark-notification.ps1 -Subscribers 100 -Seed 20261111 -SkusPerSubscriber 20 -DrainSeconds 300 -Workload default|stress`, each run after its own baseline reset. The order was `cp2/default-1`, `cp2/default-2`, `cp2/stress-1`, then the scaling and CP4 runs.

**Idle gate**: lifted for this batch on the user's instruction ("run now"). The CPU limit was `-IdleCpuCores 64`; the blocking processes (`WardogsClient-Win64-Shipping`, `ROTTR`, `Arknights`) and the port check still applied. The game that had held the 2-core gate for hours had closed by then. `run.json` records background CPU of 0.54, 0.50, and 0.25 cores, within the usual 2-core gate.

**Validation**: every run passed:
- the validator passed for all three 006 services;
- reconciliation `--mode=fixed` passed (36,189, 34,316, and 86,499 owed notifications, all delivered and received, with 0 redeliveries);
- the drain finished in about 2 s, with 0 parked events, 0 final consumer lag, and 0 background completions.

Every stress Complete error is a 409 `INSUFFICIENT_STOCK` (3669 lines of `client-stderr.txt`). The default runs had none.

**Comparison with CP1: a drop of more than 5%, investigated.** The CP2 default median is 3742, −13.9% against CP1's 4346; stress is −9.3% (9784 vs. 10787). Complete p95 rose from about 71 ms to about 101 ms.

To separate the machine from the code, two control runs repeated CP1 exactly: `server/benchmark-event-based.ps1 -Workload default`, Notification not running, with the normal idle gate (`-IdleCpuCores 2.0`, background 0.10 and 0.08 cores). They ran right after the CP4 runs, as `n007-cp2-control-{1,2}`, moved into `cp2/control-{1,2}/`. Both are `comparable: true`, with `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`.
- **The machine**: the controls gave 4152 and 3926, median 4039. That is 7.1% below CP1's 4346 with the same code path, so the machine was slower on this day than on 2026-11-12. This half of the drop is not attributable to Notification.
- **Notification's own cost**: against the same day's control, the four default runs with Notification and 100 subscribers (`cp2/default-1` 3840, `cp2/default-2` 3643, `scaling/100` 3841, and `cp4/default` 3523; median 3742) are 7.4% lower. Complete p95 is about 100–111 ms against the controls' 73–75 ms; Start and Scan p95 rose by 2–3 ms.
- **The cost does not grow with subscribers**: the scaling runs with 10, 25, and 50 subscribers gave 3797, 3222, and 3793. The cost comes from running the fourth service at all. It shares the CPU and the one MySQL server (with a durable flush per commit) with checkout: per sale it commits a selection, writes the sale's notification rows, and marks deliveries. The extra commits compete for the same redo log as Transaction's T3, which is what Complete latency waits on. This explanation fits the latency shift but was not isolated further.

**Decision**: the drop is recorded, not fixed in this phase. Correctness is unaffected: every run reconciles, and checkout keeps no dependency on Notification (US6). The design's estimate of at most 3% (research N15) covered the `SaleCompleted` row, which CP1 measured at +0.1%; it did not cover a fourth service on the same host and database. Reducing it would mean tuning Notification's commit rate (the CP3 settings) or giving it its own database server, which the single-machine deployment does not provide. The final README states this cost next to the CP4 numbers.

## CP3 (T123, optional): not run

The optional CP3 A/B would compare `NOTIFICATION_SALE_CONSUMER_THREADS` 4 vs. 8, `NOTIFICATION_LANE_IN_FLIGHT` 32 vs. 64, and a marking interval of 20 vs. 50 ms. It was not run, so every setting keeps its default. `server/benchmark-compare.ps1` drives only feature 006's harness, which does not start Notification, so CP3 needs a pairing driver for `benchmark-notification.ps1` first. It also needs about twelve more runs on the shared machine. The CP2 finding above (a cost that does not depend on the subscriber count) makes the marking interval the setting most worth trying first, if the cost is taken up later.

## CP4 (T126)

**Date**: 2026-11-13, in the same batch as CP2, each run after its own baseline reset: `cp4/default`, then `cp4/stress`.

**Results**: default 3523 transactions (0 errors); stress 9210 (2775 Complete errors, all 409 `INSUFFICIENT_STOCK`). Background CPU was 0.60 and 0.19 cores. Both validators passed, and both reconciliations passed (33,520 and 83,393 owed notifications, all received once). The drain took about 2 s.

The official copies are in this folder:
- the Load Client reports, byte-identical: `default-report.json` and `stress-report.json`;
- `default-notification-report.{json,md}`, `default-reconciliation.json`, and the stress equivalents;
- `db/validate-invariants.ps1`'s output, `validator.txt`, in each run folder.

Against feature 006's official CP4 (default 4371, stress 10553), the default run is 19.4% lower and the stress run 12.7% lower. About 7% of that is this day's machine, measured by the CP2 controls; the rest is Notification's cost, analyzed in CP2.
