# Monolithic vs Layered Benchmark

Run date: 2026-09-23 (America/Los_Angeles)

## Method

- Monolithic implementation: committed `monolithic-architecture` branch at `d130d13e5b9bb99e378a38faff7fedaf15a4538b`.
- Layered implementation: current uncommitted `layered-architecture` working tree.
- Three independent runs per architecture and workload.
- Default workload: 10 stations for 60 seconds.
- Stress workload: 100 stations for 120 seconds.
- Every run used the same Java 21 runtime, unchanged load client, 10-connection pool, and isolated MySQL 8.4 server.
- The database and server process were recreated/restarted before every run.
- Benchmark services used ports 3310 and 18080 to avoid the user's existing services on 3307 and 8080.

All values below are medians of three runs. Lower latency is better; higher throughput is better.

## Default workload medians

| Metric | Monolithic | Layered | Layered difference |
|---|---:|---:|---:|
| Transactions/s | 307.14 | 315.51 | +2.73% |
| Items/s | 6,964.74 | 7,216.50 | +3.61% |
| Start mean | 4.57 ms | 4.43 ms | 3.16% faster |
| Start p95 | 7.42 ms | 7.33 ms | 1.23% faster |
| Scan p95 | 0.287 ms | 0.272 ms | 5.16% faster |
| Complete mean | 8.34 ms | 8.05 ms | 3.42% faster |
| Complete p95 | 16.68 ms | 16.77 ms | 0.57% slower |
| Complete p99 | 37.20 ms | 36.79 ms | 1.08% faster |
| Completion error rate | 53.62% | 54.26% | +0.64 percentage points |

Default conclusion: layered is slightly faster overall. Completion p95 is effectively tied.

## Stress workload medians

| Metric | Monolithic | Layered | Layered difference |
|---|---:|---:|---:|
| Transactions/s | 252.25 | 253.45 | +0.47% |
| Items/s | 8,296.54 | 8,320.68 | +0.29% |
| Start mean | 60.38 ms | 57.24 ms | 5.21% faster |
| Start p95 | 143.67 ms | 137.55 ms | 4.26% faster |
| Start p99 | 205.74 ms | 225.00 ms | 9.36% slower |
| Scan p95 | 0.284 ms | 0.283 ms | effectively tied |
| Complete mean | 66.23 ms | 79.69 ms | 20.33% slower |
| Complete p95 | 154.38 ms | 191.34 ms | 23.94% slower |
| Complete p99 | 211.10 ms | 287.93 ms | 36.40% slower |
| Completion error rate | 67.88% | 67.87% | effectively tied |

Stress conclusion: throughput is tied, but monolithic has materially better completion latency, especially at p95/p99.

## Variability

| Architecture/workload | TPS range | Complete p95 range | Complete p99 range |
|---|---:|---:|---:|
| Monolithic/default | 305.18–312.48 | 15.66–17.16 ms | 32.61–38.01 ms |
| Layered/default | 313.90–316.04 | 16.29–17.51 ms | 35.33–38.97 ms |
| Monolithic/stress | 248.36–257.35 | 154.29–157.98 ms | 209.85–216.19 ms |
| Layered/stress | 250.68–258.97 | 180.14–194.56 ms | 268.28–306.23 ms |

The non-overlapping stress completion ranges make that regression more persuasive than a single-run difference. Completion errors in these workloads are expected `INSUFFICIENT_STOCK` conflicts after highly popular SKUs reach zero; start and scan operations recorded zero errors. The final layered stress database passed the project's invariant validation (`PASS`, non-negative inventory and reconciled completed quantities).

## Verdict

- Choose layered for the default workload and maintainability: it has modestly higher normal-load throughput with comparable latency.
- Choose monolithic for raw high-load response time: it delivers the same stress throughput with substantially lower completion p95/p99.
- Overall performance winner: monolithic under stress; otherwise the implementations are close.

Raw per-run values are in `results.csv`, median values are in `median-summary.csv`, and all 12 client-generated JSON reports are under `reports/`.
