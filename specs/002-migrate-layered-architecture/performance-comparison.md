# Performance Comparison: Monolithic vs Layered

**Benchmark date:** 2026-09-23  
**Layered branch:** `layered-architecture`  
**Monolithic baseline:** commit `d130d13e5b9bb99e378a38faff7fedaf15a4538b`

## Purpose

This comparison measures whether migrating the server from its monolithic package structure to the
four-layer design changed observable load-test performance. It uses repeated trials instead of the
single default and stress reports required by the assignment.

## Controlled method

- The committed monolithic implementation and current layered implementation were compiled with
  Java 21.
- Both used the unchanged load client and the same MySQL 8.4 process.
- Both used their default 10-connection database pool.
- Each architecture ran three default trials (10 stations for 60 seconds) and three stress trials
  (100 stations for 120 seconds).
- Before every trial, the database was recreated from `db/init.sql` with 2,000 SKUs and 10,000 units
  per SKU, and a new server process was started.
- Benchmark-only ports 3310 and 18080 isolated the runs from services already using ports 3307 and
  8080.
- The tables below report the median of three trials. Higher throughput and lower latency are better.

The final layered stress database passed `db/validate-invariants.sql`: inventory remained
non-negative, completed quantities reconciled with stock reductions, and completion state remained
atomic. All twelve server stderr logs were empty.

## Default workload: 10 stations for 60 seconds

| Metric | Monolithic median | Layered median | Layered difference |
|---|---:|---:|---:|
| Transactions per second | 307.14 | 315.51 | **+2.73%** |
| Items per second | 6,964.74 | 7,216.50 | **+3.61%** |
| Start mean | 4.57 ms | 4.43 ms | 3.16% faster |
| Start p95 | 7.42 ms | 7.33 ms | 1.23% faster |
| Start p99 | 16.08 ms | 16.14 ms | 0.40% slower |
| Scan mean | 0.213 ms | 0.202 ms | 4.94% faster |
| Scan p95 | 0.287 ms | 0.272 ms | 5.16% faster |
| Complete mean | 8.34 ms | 8.05 ms | 3.42% faster |
| Complete p95 | 16.68 ms | 16.77 ms | 0.57% slower |
| Complete p99 | 37.20 ms | 36.79 ms | 1.08% faster |
| Completion error rate | 53.62% | 54.26% | +0.64 percentage points |

### Default-load analysis

The layered implementation has a small but consistent throughput advantage: 2.73% for completed
transactions and 3.61% for scanned items. Most latency differences are below one millisecond, and
completion p95 differs by less than 0.1 ms. The practical conclusion is that the migration did not
add meaningful normal-load latency and may have improved throughput slightly.

The completion error-rate difference is not evidence of lower server reliability. These responses
are expected `INSUFFICIENT_STOCK` conflicts after the Zipf-distributed workload exhausts hot SKUs.
Start and scan operations recorded zero errors in every report.

## Stress workload: 100 stations for 120 seconds

| Metric | Monolithic median | Layered median | Layered difference |
|---|---:|---:|---:|
| Transactions per second | 252.25 | 253.45 | **+0.47%** |
| Items per second | 8,296.54 | 8,320.68 | **+0.29%** |
| Start mean | 60.38 ms | 57.24 ms | 5.21% faster |
| Start p95 | 143.67 ms | 137.55 ms | 4.26% faster |
| Start p99 | 205.74 ms | 225.00 ms | 9.36% slower |
| Scan mean | 0.215 ms | 0.210 ms | 2.44% faster |
| Scan p95 | 0.284 ms | 0.283 ms | effectively tied |
| Complete mean | 66.23 ms | 79.69 ms | **20.33% slower** |
| Complete p95 | 154.38 ms | 191.34 ms | **23.94% slower** |
| Complete p99 | 211.10 ms | 287.93 ms | **36.40% slower** |
| Completion error rate | 67.88% | 67.87% | effectively tied |

### Stress-load analysis

Stress throughput is effectively identical: the layered differences of 0.47% transactions per
second and 0.29% items per second are small relative to normal run-to-run variation. Start p95 is
slightly better in the layered implementation, and scan latency is unchanged.

Completion latency is the material regression. Layered completion is 20.33% slower at the mean,
23.94% slower at p95, and 36.40% slower at p99. This regression appears in all three trials rather
than being caused by one outlier:

| Architecture/workload | TPS range | Complete p95 range | Complete p99 range |
|---|---:|---:|---:|
| Monolithic/default | 305.18–312.48 | 15.66–17.16 ms | 32.61–38.01 ms |
| Layered/default | 313.90–316.04 | 16.29–17.51 ms | 35.33–38.97 ms |
| Monolithic/stress | 248.36–257.35 | **154.29–157.98 ms** | **209.85–216.19 ms** |
| Layered/stress | 250.68–258.97 | **180.14–194.56 ms** | **268.28–306.23 ms** |

The non-overlapping stress completion ranges make the tail-latency regression more persuasive than
a single-run comparison. A likely next investigation is the layered completion path and its extra
service/store boundary calls; the reports establish the regression but do not by themselves prove
its internal cause.

## Conclusion

- **Default load:** layered is slightly better, with modestly higher throughput and effectively
  equivalent latency.
- **Stress throughput:** tied; neither architecture completes meaningfully more work.
- **Stress response time:** monolithic is better because completion p95 and p99 are materially lower.
- **Architecture decision:** the layered version preserves throughput and provides the required
  separation of responsibilities, but currently pays a high-load completion tail-latency cost that
  should be profiled if performance optimization is in scope.

## Evidence

The complete benchmark bundle is at
[`benchmark-runs/20260923-architecture-comparison/`](../../benchmark-runs/20260923-architecture-comparison/README.md):

- [`results.csv`](../../benchmark-runs/20260923-architecture-comparison/results.csv) contains all
  per-run metrics.
- [`median-summary.csv`](../../benchmark-runs/20260923-architecture-comparison/median-summary.csv)
  contains the four median rows used above.
- [`run-benchmarks.ps1`](../../benchmark-runs/20260923-architecture-comparison/run-benchmarks.ps1)
  records the reproducible orchestration procedure.
- [`reports/monolithic/default/`](../../benchmark-runs/20260923-architecture-comparison/reports/monolithic/default/)
  and [`reports/monolithic/stress/`](../../benchmark-runs/20260923-architecture-comparison/reports/monolithic/stress/)
  contain the six monolithic reports.
- [`reports/layered/default/`](../../benchmark-runs/20260923-architecture-comparison/reports/layered/default/)
  and [`reports/layered/stress/`](../../benchmark-runs/20260923-architecture-comparison/reports/layered/stress/)
  contain the six layered reports.

### Raw report files

| Architecture | Default trials | Stress trials |
|---|---|---|
| Monolithic | `181406`, `181524`, `181642` | `181900`, `182118`, `182336` |
| Layered | `182454`, `182612`, `182730` | `182948`, `183206`, `183424` |

Each identifier expands to `report-20260923-<identifier>.json` in the corresponding evidence
directory above.
