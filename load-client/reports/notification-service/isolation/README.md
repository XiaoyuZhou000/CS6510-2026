# Isolation runs (SC-006)

Written by `subscriber-client/run.sh IsolationSummary --dir=<this folder>` (T125). Each run is `server/benchmark-notification.ps1 -Workload default -Subscribers 100 -Seed 20261111 -FaultSubscriber sub-0001 -Fault none|reject|stall`. The latency is each run's `latencyMs.endToEndExcluding.p95`: the p95 end-to-end latency of every subscriber except the faulted one. SC-006 needs at least 3 runs per mode, a faulted median at most 1.20 of the healthy median, and every run reconciled with zero missing and zero duplicate notifications (a faulted run reconciles after its fault is cleared and its parked notifications are retried).

**SC-006: PASS**

| Mode | Runs | Median p95 excluding (ms) | Ratio to healthy |
|---|---:|---:|---:|
| healthy | 3 | 97 | — |
| reject | 3 | 98 | 1.01 |
| stall | 3 | 94 | 0.97 |

| Run | Faults | p95 excluding (ms) | Reconciliation | Missing | Duplicate |
|---|---|---:|---|---:|---:|
| [healthy-1](healthy-1/notification-report.md) | `none` | 97 | PASS | 0 | 0 |
| [healthy-2](healthy-2/notification-report.md) | `none` | 94 | PASS | 0 | 0 |
| [healthy-3](healthy-3/notification-report.md) | `none` | 98 | PASS | 0 | 0 |
| [reject-1](reject-1/notification-report.md) | `reject:sub-0001` | 93 | PASS | 0 | 0 |
| [reject-2](reject-2/notification-report.md) | `reject:sub-0001` | 98 | PASS | 0 | 0 |
| [reject-3](reject-3/notification-report.md) | `reject:sub-0001` | 100 | PASS | 0 | 0 |
| [stall-1](stall-1/notification-report.md) | `stall:sub-0001` | 100 | PASS | 0 | 0 |
| [stall-2](stall-2/notification-report.md) | `stall:sub-0001` | 94 | PASS | 0 | 0 |
| [stall-3](stall-3/notification-report.md) | `stall:sub-0001` | 85 | PASS | 0 | 0 |
