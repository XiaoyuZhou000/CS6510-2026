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
| CP2 | `cp2-default-1` | default | 4021 | −16.5 | 12.7 / 26.3 | 14.5 / 44.8 | 74.6 / 138.6 | 0 | 0 | 0 | 0 | 1 | 1 | [cp2-default-1](cp2-default-1/) |
| CP2 | `cp2-stress-1` | stress | 10847 | −5.1 | 24.7 / 62.3 | 29.4 / 72.2 | 1463.5 / 1972.0 | 5941 | 0 | 0 | 0 | 1 | 1 | [cp2-stress-1](cp2-stress-1/) |
| CP2 | `cp2-default-2` | default | 4043 | −16.1 | 13.0 / 28.0 | 14.7 / 43.8 | 82.3 / 145.7 | 0 | 0 | 0 | 0 | 1 | 1 | [cp2-default-2](cp2-default-2/) |
| CP4 | `cp4-default-1` (official) | default | 4371 | −9.2 | 12.1 / 27.6 | 13.6 / 41.8 | 71.0 / 140.0 | 0 | 0 | 0 | 0 | 1 | 1 | [cp4-default-1](cp4-default-1/) |
| CP4 | `cp4-stress-1` (official) | stress | 10553 | −7.6 | 23.4 / 60.7 | 26.0 / 66.9 | 1427.1 / 2671.6 | 5954 | 0 | 0 | 0 | 1 | 1 | [cp4-stress-1](cp4-stress-1/) |
| CP0 (T166) | `cp4-vs-cp0-default-a-1` | default | 4930 | +2.4 | 11.2 / 23.4 | 12.4 / 32.6 | 53.3 / 101.4 | 0 | 0 | | | 1 | 1 | [cp4-vs-cp0-default-a-1](cp4-vs-cp0-default-a-1/) |
| CP4 (T166) | `cp4-vs-cp0-default-b-1` | default | 4560 | −5.3 | 11.6 / 32.3 | 13.6 / 42.5 | 71.4 / 137.8 | 0 | 0 | 0 | 0 | 1 | 1 | [cp4-vs-cp0-default-b-1](cp4-vs-cp0-default-b-1/) |
| CP0 (T166) | `cp4-vs-cp0-stress-a-1` | stress | 11423 | 0.0 | 18.5 / 50.0 | 20.7 / 49.9 | 969.8 / 1386.3 | 7261 | 0 | | | 1 | 1 | [cp4-vs-cp0-stress-a-1](cp4-vs-cp0-stress-a-1/) |
| CP4 (T166) | `cp4-vs-cp0-stress-b-1` | stress | 10725 | −6.1 | 24.1 / 63.5 | 26.5 / 69.6 | 1524.9 / 2067.1 | 6252 | 0 | 0 | 0 | 1 | 1 | [cp4-vs-cp0-stress-b-1](cp4-vs-cp0-stress-b-1/) |

## Summary

| Checkpoint | Workload | Runs | `totalTransactions` median | Range (min–max) |
|---|---|---:|---:|---|
| CP0 | default | 2 | 4816.5 | 4736–4897 |
| CP0 | stress | 1 | 11427 | 11427 |
| CP1 | default | 2 | 4354.5 (−9.6) | 4312–4397 |
| CP1 | stress | 1 | 10711 (−6.3) | 10711 |
| CP2 | default | 2 | 4032 (−16.3; −7.4 vs CP1) | 4021–4043 |
| CP2 | stress | 1 | 10847 (−5.1; +1.3 vs CP1) | 10847 |
| CP3 | default | 4 (variant A runs) | 4445.5 (−7.7) | 4180–4559 |
| CP3 | stress | 2 (variant A runs) | 10583.5 (−7.4) | 10231–10936 |
| CP4 | default | 2 (official + T166) | 4465.5 (−7.3) | 4371–4560 |
| CP4 | stress | 2 (official + T166) | 10639 (−6.9) | 10553–10725 |

The Δ in parentheses is against the CP0 median of 2026-10-29. The design comparison is the interleaved T166 pair in *CP4* below, where CP0 was measured again in the same hour.

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

## CP2: full event-based system (T131)

**Measured**: 2026-10-30, 03:52–04:02 local time. Three runs: `cp2-default-1` and `cp2-stress-1` back to back, then `cp2-default-2`, taken because the first default run fell more than 5% below CP1.

**Implementation**: the working tree of phase 6 (US5) on top of commit `ab6b53a`, uncommitted, so `evidence.json` reports `gitDirty: true`. Every inter-service interaction is now an event: the checkout Saga (US1, US3), the catalog (US4), and the scans (US5). The scan relay publishes `ScanAccepted` records through `KafkaScanEventSink`, and Analytics consumes them in batches with `consumer_offset` committed in the ingestion transaction. Inventory and Analytics serve no gRPC.

**Command**: `server/benchmark-event-based.ps1 -Workload default|stress -OutputLabel <label>`, with `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR`. The readiness step now also waits for `analytics-scan-ingest`, after Analytics has started; a first attempt that waited for it before Analytics started timed out during setup and measured nothing, and its output was removed.

**Environment**: as for CP1. The idle gate passed for every run, but the background load was higher than for CP1: 0.89, 0.50, and 0.76 cores against 0.44–0.45. The heaviest process was the game `Arknights` at 0.34–0.36 cores, which is not on the `-BlockingProcess` list.

**Validation**:
- Every run is `comparable: true`: the validator passed, all pending work drained, zero parked events in all three services, and zero final consumer lag. The scan group's peak lag in the 10 s samples was 20, 35, and 110 records.
- Zero background completions and zero completions without a recorded response (FR-061).
- The stress run's 5941 Complete errors are 409 `INSUFFICIENT_STOCK` once the hot SKU sells out, as in CP0 (7321) and CP1 (5879).

### Investigation of the default drop (more than 5% below CP1)

**Default, −7.4% against CP1 (two runs, 4021–4043); stress +1.3%**:
- **The database work is unchanged**. Per run, the relay made 3043 marking commits (CP1: 3059 delivery rounds) and Analytics 3037 ingestion commits (CP1: 3059). Each ingestion adds one `consumer_offset` upsert to a transaction that already exists, so no commit was added. The Scan commit's p50 stayed at 7.6 ms.
- **Start slowed most, although nothing in this phase touches it**. Start p50 rose from 6.8–6.9 to 8.0 ms (+16%), Scan p50 from 8.0–8.1 to 8.5–8.6 ms (+6%), and Complete p50 from 29.1–29.2 to 29.7–29.8 ms (+2%). Start is one INSERT and one commit, with no broker or relay involvement.
- **The machine was busier**. Background CPU roughly doubled because of the game, and the broker now also carries about 700 scan records per second plus the Analytics consumer's fetches. The two cannot be separated from these runs.
- **Stress did not drop**. It is +1.3% against CP1, and Inventory's consumer lag p95 fell from 1638 to 1378 ms. Under stress the hot SKU's row lock dominates, not CPU.

**Conclusion**: no correctness regression, and no extra database work on any path. The default drop matches the higher background load, together with the broker's added scan traffic. It must be measured again on an idle machine before any cost is attributed to the scan path. CP3 (T163) and CP4 (T164–T166) run on an idle machine with the same harness.

### D17 entry criteria

| Option | Criterion | CP2 | Result |
|---|---|---|---|
| Inventory group commit | Inventory consumer lag p95 > 50 ms under stress | 1378 ms in `cp2-stress-1` (p50 410 ms) | **Met**: a candidate for the CP3 A/B (T163); not adopted here |
| Immediate publish for scans | Visibility p99 > 5 s (SC-008) | Under 0.5 s: relay latency p99 79–133 ms, scan consumer lag p99 30–79 ms, and window publication lag p99 157–248 ms. `integration/AnalyticsVisibilityLagTest` measured 127 ms at p99. | Not met |
## CP3: tuning A/B (T163)

**Measured**: 2026-10-30, 19:32–19:49, then 2026-10-31, 03:53–04:46 local time. The user paused the campaign between the two sessions to use the machine. The interrupted `cp3-threads-default-a-1` was moved to `compare/cp3-threads-default/interrupted/` and run again from a fresh reset. Its first repeat was invalidated because the game `Arknights` ran during it, and the second repeat is the one recorded.

**Implementation**: commit `2ac2c07` (phase 9) with the uncommitted phase 10 documentation and diagram test, so `evidence.json` reports `gitDirty: true`. The service jars are the ones built by the phase 9 gate. No source file changed.

**Command**: `server/benchmark-compare.ps1 -Workload <w> -Label <label> -VariantA @{...} -VariantB @{...} -Pairs 1 -IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`. `Arknights` was added to the blocking list after it loaded CP2. Each run starts from its own `db/reset-baseline.ps1`. The policy is D15's streamlined one: one screening pair per relevant workload, and three confirmation pairs only at ≥ +5%. Flush and consumer threads act on both the request path and the broker, so they ran on both workloads. The scan linger and the relay coalescing act only on the off-path scan relay, whose batches are already full under stress, so they ran on the default workload only.

**Validation**: every recorded run is `comparable: true`: the validator passed, all work drained, zero parked events, zero final consumer lag, and zero background completions. Background CPU was 0.12–1.03 cores per run. Stress Complete errors are 409 `INSUFFICIENT_STOCK` once the hot SKU sells out.

| Comparison | Workload | A (default) | B (variant) | A `totalTransactions` | B `totalTransactions` | Δ | Complete p95 / p99, A → B (ms) | Classification | Decision |
|---|---|---|---|---:|---:|---:|---|---|---|
| [`cp3-flush-default`](compare/cp3-flush-default/) | default | `KAFKA_FLUSH_EVERY_MESSAGE=false` | `true` | 4180 | 2929 | −29.93% | 74.8 / 140.6 → 128.3 / 211.5 | regression; Scan p99 +22.5%, Complete p99 +50.4% | keep `false` |
| [`cp3-flush-stress`](compare/cp3-flush-stress/) | stress | `false` | `true` | 10936 | 9179 | −16.07% | 1470 / 2109 → 2008 / 2805 | regression; Scan p99 +14.1%, Complete p99 +33.0% | keep `false` |
| [`cp3-threads-default`](compare/cp3-threads-default/) | default | `CHECKOUT_CONSUMER_THREADS=8` | `4` | 4422 | 4520 | +2.22% | 77.0 / 132.3 → 72.9 / 129.4 | no demonstrated improvement (within ±3%) | keep 8 |
| [`cp3-threads-stress`](compare/cp3-threads-stress/) | stress | `8` | `4` | 10231 | 10655 | +4.14% | 1723 / 2424 → 1600 / 2211 | no demonstrated improvement (below +5%) | keep 8 |
| [`cp3-linger-default`](compare/cp3-linger-default/) | default | `SCAN_PRODUCER_LINGER_MS=5` | `20` | 4559 | 4412 | −3.22% | 68.4 / 121.6 → 70.4 / 131.2 | regression | keep 5 |
| [`cp3-coalesce-default`](compare/cp3-coalesce-default/) | default | `OUTBOX_COALESCE_MS=0` | `100` | 4469 | 4281 | −4.21% | 65.1 / 123.0 → 68.5 / 129.9 | regression | keep 0 |

**Findings**:
- **Flush per message** costs the most. The broker's `fsync` per append lengthens every produce confirmation, so both Saga hops wait longer. Inventory's checkout-consumer lag p95 rose from 7.6 to 30.4 ms at default, and Complete p95 rose by about 70%. D17's criterion for making it the default (within ±3%) is clearly not met. The broker keeps the page-cache default, and durability rests on `acks=all` plus the outboxes, which re-send anything unconfirmed (D5).
- **Four consumer threads** were ahead in both pairs (+2.2% default, +4.1% stress), with slightly lower Complete latency and fewer row-lock waits under stress. Both are under the +5% bar for a confirmation round. The spread of the default A runs across this campaign (4180–4559, 9%) is larger than either gain. The default stays 8. A smaller thread count can be revisited if a later feature needs the headroom.
- **Scan linger 20 ms and coalescing 100 ms** cut commits per transaction (16.34 → 15.84 and 16.41 → 15.57) but lost `totalTransactions`. Fewer, larger relay batches do not help when those commits are off the request path. Scan visibility is not at risk either way (CP2: under 0.5 s at p99).
- **Inventory group commit** (D17, its criterion met at CP2) is not implemented, so CP3 does not measure it. It remains a deferred option. The stress results show the hot SKU's row lock as the limit: Inventory's lock wait p99 was 112–266 ms.

**Adopted**: nothing. Every default stays as documented: flush off, 8 consumer threads, linger 5 ms, coalescing off. Code, `deploy/kafka/start-broker.ps1`, and `server/README.md` need no change.

## CP4 designation (T164–T166)

Designated before any CP4 run, on 2026-10-31:
- **Official default report**: the client report of `cp4-default-1` (`server/benchmark-event-based.ps1 -Workload default -OutputLabel cp4-default-1`, with the idle gate `-IdleCpuCores 2.0 -BlockingProcess WardogsClient-Win64-Shipping,ROTTR,Arknights`). If it is not comparable or breaks SC-013, the next attempt is `cp4-default-2`, and so on. Every attempt stays in this file.
- **Official stress report**: the client report of `cp4-stress-1`, run after its own fresh reset with the same gate. Retries follow the same rule (`cp4-stress-2`, …).
- **Final comparison**: `server/benchmark-compare.ps1 -BaselineCommit 8891c57`, one interleaved pair per workload, labels `cp4-vs-cp0-default` and `cp4-vs-cp0-stress`. These pairs describe the change between the designs. The official reports above are separate runs.

## CP4: official runs and the final comparison (T164–T166)

**Measured**: 2026-10-31. The official runs took place at 04:48–04:55 local time and the interleaved comparison at 04:56–05:09, all on an idle machine. Background CPU was 0.08–0.46 cores, and no blocking process ran.

**Implementation**: commit `2ac2c07` with the phase 10 documentation, diagram test, and harness fix uncommitted (`gitDirty: true`). The service jars are those of the phase 9 gate. Their SHA-256 values are in each `evidence.json` and in the [README](README.md). Every setting is at its documented default, because CP3 adopted nothing.

**Official runs** (designated above, first attempts):
- **`cp4-default-1`**: 4371 transactions, 72.7 completed per second. Zero errors on every operation, zero background completions, zero completions without a recorded response, zero parked events, and zero final lag. The validator passed for all three services. SC-013 is met.
- **`cp4-stress-1`**: 10553 transactions, 87.6 per second. All 5954 Complete errors are 409 `INSUFFICIENT_STOCK` after the hot SKU sold out, and Start and Scan had zero errors. Zero background completions (FR-066), zero parked events, and zero final lag. The validator passed. Inventory's checkout-consumer lag reached p95 1378 ms and p99 2527 ms behind the hot SKU's row lock (lock wait p99 223 ms). Every Complete still settled within its request: the outcome wait's p99 of 2527 ms stayed inside the 8 s `CHECKOUT_RESPONSE_BUDGET_MS`.

**Final comparison** (`server/benchmark-compare.ps1 -BaselineCommit 8891c57`, one interleaved pair per workload). Variant A runs commit `8891c57`, the feature 005 release, through its own `server/benchmark-microservices.ps1` from the worktree `../cs6510-baseline-8891c57`, built for this step. The first attempt stopped before any run: `Initialize-BaselineWorktree` read `$LASTEXITCODE` = −1 after `git rev-parse … | Select-Object -First 1` cut the pipeline. The harness now collects git's output before taking the first line, and the second attempt ran both pairs.

| Workload | CP0 (feature 005, `8891c57`) | CP4 (feature 006) | Δ | Start p95 / p99, CP0 → CP4 (ms) | Scan p95 / p99 | Complete p95 / p99 |
|---|---:|---:|---:|---|---|---|
| default | 4930 | 4560 | **−7.51%** | 11.2 / 23.4 → 11.6 / 32.3 | 12.4 / 32.6 → 13.6 / 42.5 | 53.3 / 101.4 → 71.4 / 137.8 |
| stress | 11423 | 10725 | **−6.11%** | 18.5 / 50.0 → 24.1 / 63.5 | 20.7 / 49.9 → 26.5 / 69.6 | 969.8 / 1386.3 → 1524.9 / 2067.1 |

Every run in the table is comparable by its own harness: the validators passed, all work drained, there were zero background completions, and durability was `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`. One pair per workload describes the change between the designs and claims no significance.

**Explanation**:
- **The cost is durable messaging, as CP1 attributed**. Complete now waits for two broker hops, each a produce with `acks=all` plus a consumer fetch, and for the commits on both sides. Its p95 rose by 18 ms at default. The two outbox INSERTs ride inside existing transactions, but the dispatchers' marking commits share the same redo-log and binlog flushes as Start and Scan. That is why the unchanged Start and Scan paths also lost a little at p99.
- **Stress**: the hot SKU's row lock is the limit in both designs. With events, the waiting moves from 005's per-request RPC into Inventory's consumer partitions, where a request waits behind earlier applies of its partition (head-of-line). Complete p95 therefore rose more than throughput fell.
- **The official runs are 2–4% below their comparison twins** (4371 against 4560, and 10553 against 10725), within the run-to-run spread seen in CP3 (4180–4559 for identical default settings).
- **What the cost buys**: no service calls another, each service runs and restarts without its peers (US7), Scan stays durable without the broker (US2), and every message is delivered at least once and applied at most once (US3, US5).
