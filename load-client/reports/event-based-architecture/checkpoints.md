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
| CP1 | `cp1-default-1` | default | 4397 | −8.7 | 12.3 / 35.2 | 13.6 / 40.4 | 66.4 / 130.0 | 0 | 0 | 0 | 0 | 1 | 1 | [cp1-default-1](cp1-default-1/) |
| CP1 | `cp1-default-2` | default | 4312 | −10.5 | 12.1 / 25.8 | 14.1 / 43.6 | 71.6 / 126.8 | 0 | 0 | 0 | 0 | 1 | 1 | [cp1-default-2](cp1-default-2/) |
| CP1 | `cp1-stress-1` | stress | 10711 | −6.3 | 22.4 / 55.9 | 25.9 / 65.0 | 1724.8 / 2400.7 | 5879 | 0 | 0 | 0 | 1 | 1 | [cp1-stress-1](cp1-stress-1/) |

## Summary

| Checkpoint | Workload | Runs | `totalTransactions` median | Range (min–max) |
|---|---|---:|---:|---|
| CP0 | default | 2 | 4816.5 | 4736–4897 |
| CP0 | stress | 1 | 11427 | 11427 |
| CP1 | default | 2 | 4354.5 (−9.6) | 4312–4397 |
| CP1 | stress | 1 | 10711 (−6.3) | 10711 |

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

## CP1: Saga over events (T079)

**Measured**: 2026-10-30, 00:28–00:39 local time. Three runs: `cp1-default-1` and `cp1-stress-1` back to back, then `cp1-default-2`.

**Implementation**: commit `e3cd2b4` plus the uncommitted US1 and US3 changes of this feature, so `evidence.json` reports `gitDirty: true`.
- Checkout runs as the choreographed Saga over the broker: T1 records `CheckoutRequested`, and Inventory records its outcome event.
- The catalog and the scan relay still use gRPC (plan stage 3).
- The code already contains the US3 production changes: the outcome handler's integrity branches, the stuck alarm, and the status counts. None of them adds work on the request or consumer hot path. The task asked for CP1 between US1 and US3; it is taken after both because they were implemented together, and the US3 fault and wait suites do not run during a benchmark.

**Command**: `server/benchmark-event-based.ps1 -Workload default|stress -OutputLabel <label>`, with `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR`. The idle gate passed for every run: background CPU was 0.44 cores, and no blocking process was seen.

**Environment**: as for CP0, with the native broker added: Kafka 4.1.0 in KRaft mode at the broker's default flush, reset by `db/reset-baseline.ps1` before each run.

**Validation**:
- Every run is `comparable: true`: the validator passed, all pending work drained, zero parked events, and zero final consumer lag.
- Zero background completions and zero completions without a recorded response (FR-061).
- The stress run's 5879 Complete errors are 409 `INSUFFICIENT_STOCK` once the hot SKU sells out, as in CP0 (7321).

### Investigation of the drop (plan *Checkpoints*: more than 5% below CP0)

**Default, −9.6% (two runs, 4312–4397)**:
- **Every operation is slower, not only Complete**. Start p50 rose from 6.5–6.6 to 6.8–6.9 ms and Scan p50 from 7.4–7.8 to 8.0–8.1 ms, although this change touches neither path. Complete p50 rose from 25.7–26.6 to 29.1–29.2 ms.
- **The event hops themselves cost about 3 ms**. Complete's outcome wait has p50 21.5 ms, and its stages account for it:
  - Transaction's dispatcher: commit to send 0.3 ms, send to confirmation 0.4 ms.
  - Inventory's consumer: lag 1.0 ms, then the apply transaction 9.1 ms (commit 7.6 ms).
  - Inventory's dispatcher: 0.3 ms and 0.4 ms.
  - Transaction's consumer: lag 1.0 ms, then T3 8.3 ms.

  Without the waits for the two database commits, the dispatching, broker round trips, and polls add about 3 ms. That matches the Complete increase.
- **The slowdown of the unchanged Start and Scan points to shared database load**. The new design adds two outbox INSERTs per checkout (in T1 and in the apply transaction). It also adds the dispatchers' `published_at` marking commits: in `cp1-default-1`, 1407 in Transaction and 1487 in Inventory, about 48 extra commits per second next to about 73 checkouts per second. With `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`, every one of those commits waits for the same redo-log and binlog flushes as Start, Scan, and Complete. Each marking batch carries only about 3 IDs, because the 20 ms window (`OUTBOX_MARK_BATCH_MS`) closes before more confirmations arrive. The broker and the 16 consumer threads also add CPU work on the same machine.

**Stress, −6.3% (one run)**:
- Inventory's checkout consumer falls behind on the hot SKU: lag p50 344 ms and p95 1.6 s, apply p50 56 ms, of which the stock-row lock wait is p50 47 ms.
- Requests of one partition wait behind an apply that is waiting for the hot row's lock. In feature 005, each request thread waited for its own RPC. So Complete p95 rose from 944 to 1725 ms while Start and Scan rose only slightly.
- Transaction's side stays fast: outcome consumer lag p50 1.0 ms, T3 p50 8.3 ms.

**Conclusion**: the drop is the expected cost of making every message durable (FR-010, FR-011), plus head-of-line blocking per partition on the hot SKU under stress. No correctness regression was found. Candidate levers belong to the CP3 tuning A/B (T163), not to this checkpoint:
- A larger `OUTBOX_MARK_BATCH_MS`, to cut most of the marking commits.
- More `CHECKOUT_CONSUMER_THREADS` for Inventory, to reduce head-of-line waits under contention.

