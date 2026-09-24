# Quickstart: Validate the Layered Migration

This guide is for the implemented migration. It does not change the OpenAPI file or load client.

## Prerequisites

- JDK 21+ (`java` and `javac`)
- Bash (Git Bash/MSYS, WSL, Linux, or macOS)
- MySQL 8.0+ and its command-line client
- Disposable database access at the configured host (defaults: `127.0.0.1:3307`, database
  `cs6510_selfcheckout`, user `root`)

## 1. Build and run all automated gates

From the repository root:

```bash
cd server
./build.sh
./build-tests.sh
./run-tests.sh
```

Expected: architecture and isolated unit tests pass. HTTP/DB tests that self-skip because no live
server/database is available are not evidence that end-to-end validation passed; run the following
steps with MySQL and the server available.

## 2. Reset to the canonical baseline

Stop any running server, then from a PowerShell prompt at repository root run:

```powershell
./db/init-db.ps1 -MySqlUser root -MySqlHost 127.0.0.1 -Port 3307
```

Expected sanity output: 2,000 catalog items, 2,000 inventory rows, and 20,000,000 total units.
Always start a fresh server after reset so catalog cache, open baskets, scan counter, and ring buffer
also begin cleanly.

## 3. Start the layered server

```bash
./server/build.sh
./server/run.sh 8080 127.0.0.1 3307 cs6510_selfcheckout root
```

Prefer `DB_PASSWORD` rather than placing a password in shell history. Expected: catalog loads and
the server listens on port 8080.

## 4. Smoke-test the fixed contract

Run the live contract/integration suite in another shell:

```bash
cd server
./run-tests.sh
```

Verify the lifecycle and operator paths described in
[public-api-compatibility.md](contracts/public-api-compatibility.md), including transaction lookup,
low-stock threshold override, popular-item limit, missing resources, empty basket, and duplicate
completion.

Expected: response methods/statuses/shapes match the existing OpenAPI; duplicate completion changes
neither inventory nor completed lines.

## 5. Run the default workload

With the clean server still running:

```bash
cd load-client
./build.sh
./run.sh --baseUrl=http://localhost:8080 --stations=10 --duration=60 --reportDir=./reports
```

Expected: one new `reports/report-*.json` records 10 stations and 60 seconds. Retain it. Check that
operation errors are understood, inventory is non-negative, and completed quantities reconcile to
stock reductions. Latency targets are p95 start <200 ms, scan <100 ms, completion <1 s.

## 6. Reset and run the stress workload

Stop the server. Repeat steps 2 and 3 to reset the database and start a fresh server, then run:

```bash
cd load-client
./run.sh --baseUrl=http://localhost:8080 --stations=100 --duration=120 --reportDir=./reports
```

Expected: a second, distinct timestamped JSON report records 100 stations and 120 seconds. Do not
reuse the database or server process from the default run.

## 7. Final evidence checks

- `load-client/reports/` contains distinct default and stress reports produced by the unchanged
  client, each with the correct settings.
- Every SKU has non-negative stock, and `10000 - final_stock` equals completed
  `transaction_line.quantity` summed for that SKU after each clean run.
- Repeating completion creates no second completed sale or decrement.
- At controlled 500-scan boundaries, the latest persisted result has correct window bounds, top-10
  counts, and ranking for the claimed recent 1,000 scans.
- The architecture gate reports every server component in one layer with zero handler-to-database
  bypasses, reverse dependencies, or cycles.

## 8. Phase 6 validation evidence (2026-09-23)

Each workload below used the unchanged load client after `db/init-db.ps1` recreated the canonical
2,000-SKU, 20,000,000-unit baseline and a distinct `api.Main` process was started. After each run,
`db/validate-invariants.sql` reported `PASS` for non-negative inventory, per-SKU stock
reconciliation, atomic completion state, and coherent popular-window ranks.

| Report | Workload | Completed transactions / units | Popular windows / latest end | p95 start / scan / complete | Result |
|---|---|---:|---:|---:|---|
| `report-20260923-154646.json` | 10 stations × 60 s | 8,806 / 87,743 | 232 / 654,000 | 12.35 / 0.259 / 104.61 ms | Invariants PASS; all default design targets met |
| `report-20260923-160241.json` | 100 stations × 120 s | 18,742 / 154,648 | 846 / 1,077,000 | 299.94 / 0.263 / 618.88 ms | Invariants PASS; scan and completion stayed within the default-run reference targets, while start exceeded the 200 ms reference under stress |

Both runs reached a minimum stock of zero without negative inventory. Completion conflicts caused
by depleted hot SKUs were reported as `INSUFFICIENT_STOCK` and did not violate reconciliation or
partial-completion checks.

See [data-model.md](data-model.md) for invariants and
[layer-interfaces.md](contracts/layer-interfaces.md) for collaborator behavior.

## 9. Repeated monolithic-to-layered comparison

A controlled three-trial comparison was also run for both the default and stress workloads against
the committed monolithic baseline and the current layered implementation. The layered server was
2.73% faster in default transactions per second and stress throughput was effectively tied, but its
stress completion p95 and p99 were 23.94% and 36.40% slower respectively.

See [performance-comparison.md](performance-comparison.md) for the complete analysis, variability,
method, and links to all twelve raw JSON reports.
