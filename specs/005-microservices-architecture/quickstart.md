# Quickstart: Validate the Microservice Deployment

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

This guide proves the feature end to end once implementation is complete. It references the contracts instead of repeating them:
- [public HTTP](contracts/public-http-and-routing.md)
- [internal gRPC](contracts/grpc-internal-contracts.md)
- [checkout Saga](contracts/checkout-saga.md)
- [event delivery](contracts/scan-event-delivery.md)
- [deployment](contracts/service-deployment.md)

Run `server/` scripts in Git Bash and `db/` scripts in PowerShell, both from the repository root unless noted.

## 0. Prerequisites

- JDK 21 and Maven 3.9 or later on `PATH`. The first build downloads gRPC and protobuf artifacts from Maven Central.
- MySQL 8.4 on `127.0.0.1:3307` with an admin account, keeping the default `max_connections` of 151 or higher.
- Nginx for Windows or Unix, configured as in feature 004 (`server/README.md`).
- Environment variables for the three service passwords: `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, `ANALYTICS_DB_PASSWORD`.

## 1. Build

```bash
cd server
./build.sh
```

Expected: `out/contracts.jar`, `out/common.jar`, `out/transaction.jar`, `out/inventory.jar`, and `out/analytics.jar` exist, and `lib/grpc/` holds the resolved runtime jars. `./build.sh inventory` must also succeed on its own (FR-050).

## 2. Bootstrap once, then reset to the baseline

```powershell
./db/bootstrap.ps1 -AdminUser root -Port 3307
./db/reset-all.ps1 -Port 3307
```

Expected:
- The bootstrap reports three schemas and three users.
- The reset prints 2,000 catalog rows, 2,000 inventory rows, and 20,000,000 units, with zero transactions, outbox rows, and Analytics events (FR-052).

## 3. Start the services and the gateway

```bash
cd server
./run-inventory.sh
./run-transaction.sh
./run-analytics.sh
```

Run each command in its own terminal, then start Nginx as documented. Smoke-test through the gateway:

```bash
curl -s http://localhost:8080/items
```

```bash
curl -s -X POST http://localhost:8080/transactions -H "Content-Type: application/json" -d '{"stationId":"station-qs"}'
```

Expected: `/items` returns 2,000 items in SKU order, and Start returns 201 with an OPEN transaction. A scan of `SKU-000001`, a lookup, and a complete then return the established responses (SC-001).

## 4. Automated suites

```bash
cd server
./build-tests.sh
```

```bash
cd server
SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh
```

Expected: all tests pass with no skipped live contract or integration tests. The suites include:
- `tests/contract`: the seven routes, unchanged.
- `tests/rpc`: gRPC outcomes, statuses, replay, payload mismatch, deadlines, Analytics acknowledgement after commit, and compatibility (FR-013).
- `tests/integration`: concurrency, the Saga, crashes (including Analytics crashes from reception to acknowledgement), waiting, delivery, and isolation.
- `tests/architecture`: layering, `rpc` boundaries, inert fault hooks, byte-identical Load Client and OpenAPI files, no Redis or Kafka, no MySQL setting changes, and the coverage-parity ledger (FR-057).
- `tests/performance` (R22):
  - exact correctness tests: acknowledgement after commit, shared commits, no remote call on Scan, and no connection or row lock held across `ApplyCheckout` or a wait;
  - measured statement and commit budgets, with ceilings in `statement-budgets.properties`;
  - cache tests for staleness, eviction races, ambiguous commits, two instances, and restart rebuilds.
- Direct tests of the reserved restoration operation, reported in their own class (SC-012).

The coverage-parity gate fails while any legacy assertion lacks a passing equivalent (FR-057). Set `PARITY_GATE` to a story, such as `US2`, to check that story and every earlier one, or to `final` to check every row:

```bash
cd server
PARITY_GATE=final SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh
```

## 5. Scenario checks

| # | Scenario | Steps | Expected outcome |
|---|---|---|---|
| A | Checkout across services (US1) | Through the gateway: start, scan two units of A and one of B, complete | 200 receipt with captured prices; Inventory stock drops by exactly 2 and 1; lookup shows COMPLETED; a repeat Complete gives 409 `TRANSACTION_NOT_OPEN` |
| B | Insufficient stock reopens (US1, compensation path) | Integration test with a one-unit SKU and a two-unit basket | 409 `INSUFFICIENT_STOCK`; no stock change; transaction OPEN; scanning and retrying work as today |
| C | Scan without Inventory (US2, SC-004) | Stop Inventory. Start, scan, and look up a new transaction. Restart Transaction and scan again. Request Complete. | Start, Scan, and Lookup succeed, including after the restart. Complete returns 500 `INTERNAL_ERROR` within 10 s and lookup still shows OPEN. Start Inventory again: lookup shows COMPLETED shortly after, and a repeat Complete gives 409. |
| D | Interrupted checkout (US3, SC-003) | Run the fault-injection tests for each hook in the [Saga matrix](contracts/checkout-saga.md#crash-and-fault-matrix), or launch Transaction once with `-Dselfcheckout.faults=checkout.afterApplyReply:halt`, complete, then restart normally | Each attempt settles without manual steps: COMPLETED exactly when Inventory applied it. Stock changes once. No timeout triggers restoration. |
| E | Bounded waiting (FR-035) | Integration test: keep Inventory unresponsive, issue Complete, then Scan the same transaction | Scan waits, then returns 500 with no basket change. After Inventory recovers, the attempt settles and a new scan follows the settled state. |
| F | Analytics outage (US5, SC-005) | Stop Analytics during a short Load Client run, then restart it | No Scan or Complete failures. Pending work drains to zero (see [operational status](contracts/service-deployment.md#operational-status-fr-039)). Every window matches recomputation. |
| G | Isolation (US6, SC-008) | Connect as `cs6510_inventory_svc` and select from `cs6510_transaction.transaction`; repeat for every pair of services | MySQL error 1142 (command denied) for each attempt |
| H | Independent restart (US6, SC-009) | During a Load Client run, restart one service at a time | Peers keep serving their routes. After settlement, the invariants pass and no acknowledged scan is lost. |

## 6. Performance checkpoints and benchmark evidence (US7, SC-010, SC-011; R21, R22)

Each checkpoint run uses the harness described in the [deployment contract](contracts/service-deployment.md#benchmark-harness-and-checkpoints-r21-r22). The harness resets all three services, runs the unchanged Load Client, samples backlog and lock counters, waits for settlement, validates invariants, and records the MySQL durability settings.

```powershell
./server/benchmark-microservices.ps1 -Workload smoke -OutputLabel cp1-smoke
```

```powershell
./server/benchmark-microservices.ps1 -Workload default -OutputLabel cp4-default
```

```powershell
./server/benchmark-microservices.ps1 -Workload stress -OutputLabel cp4-stress
```

| Checkpoint | What to compare | Expected |
|---|---|---|
| CP0 attribution baseline (existing `server/benchmark-scan.ps1` on the current implementation, before changes: default ×3, stress ×2) | `totalTransactions` median and range, per-operation p95/p99, pool wait, row-lock counters | Recorded in `load-client/reports/microservices-architecture/checkpoints.md`; used only for attribution |
| CP1 core checkout and CP2 full system (attribution) | `totalTransactions` against the previous checkpoint; Scan and Complete server-side p95/p99; Transaction and Inventory borrow wait and hold time; Inventory locking-SELECT time; `ApplyCheckout` latency; outbox and Analytics backlog. CP2 adds recovery queries, wait checks, and idle polls. | A drop of more than 5% without a correctness reason is investigated before continuing |
| CP3 tuning A/B (`server/benchmark-compare.ps1`) | Each variant against the defaults, in interleaved pairs under equivalent workloads on its relevant workloads (pool sizes under stress): JDBC flags, server prepared statements, pool sizes, statement-budget alternatives | One screening pair per relevant workload; three confirmation pairs only when a screen shows Δ ≥ +5%; no extension. Keep a variant only with Δ ≥ +3%, ahead in at least two of three pairs, a passing default regression check, and no correctness failure or p99 regression over 10%. Record each decision and its reduced statistical confidence. |
| CP4 final comparison and official evidence | Fresh CP0 runs (worktree of the baseline commit) interleaved with CP4: 3 default pairs and 3 stress pairs, reported without claims of statistical significance. The first CP4 run of each workload is the official report, designated in advance. | Default: zero errors and zero background completions (SC-010). Stress: background completions reported and explained (FR-058). Both pass the validator (SC-002, SC-006). `evidence.json` shows `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1`. |

At every checkpoint, report the actual client-observed `totalTransactions` and its change against the CP0 baseline for the same workload. Fewer SQL statements or lower latency never count as an improvement on their own.

Copy the CP4 reports byte for byte to `load-client/reports/microservices-architecture/default.json` and `stress.json`. Document the comparison with CP0 and the retained service-based evidence in that folder's `README.md`. Cover `totalTransactions`, throughput, error rates, p95/p99, background completions, the mandatory-versus-optional cost breakdown, and Analytics catch-up.
