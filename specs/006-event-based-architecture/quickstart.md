# Quickstart: Validate the Event-Based Deployment

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Deployment**: [contracts/broker-and-deployment.md](contracts/broker-and-deployment.md)

This guide proves the feature end to end. It references the contracts instead of repeating them. Commands assume Git Bash in `server/` unless they start with `powershell`, which runs from the repository root.

## Prerequisites

- **Tools**: JDK 21, Maven 3.9 or later, MySQL 8.4 on `127.0.0.1:3307` bootstrapped with `db/bootstrap.ps1` (feature 005), and Nginx as in feature 005.
- **Broker**: Docker Desktop running (WSL 2 backend).
- **Credentials**: the per-service passwords in `TRANSACTION_DB_PASSWORD`, `INVENTORY_DB_PASSWORD`, and `ANALYTICS_DB_PASSWORD`. They are never passed as arguments.
- **No feature 005 processes**: none running, and ports 8080–8083 and 19092 free.

## 1. Start the broker and build

```bash
docker compose -f ../deploy/kafka/compose.yaml up -d
powershell -File ../deploy/kafka/create-topics.ps1
./build.sh
```

Expected: `deploy/kafka/status.ps1` lists four topics with partitions 1, 16, 16, and 1. `out/` holds `contracts.jar`, `common.jar`, and the three service jars. `lib/grpc/` no longer exists, and `lib/messaging/` holds `kafka-clients-4.1.0.jar`.

## 2. Reset the baseline and start the services

```bash
powershell -File ../db/reset-baseline.ps1
./run-inventory.sh      # terminal 1
./run-transaction.sh    # terminal 2
./run-analytics.sh      # terminal 3
```

Then start Nginx as in feature 005.

Expected:
- The reset prints 2,000 products, 20,000,000 units, and zero transactions, outbox rows, parked events, and topic offsets.
- Within 5 s, Transaction's status line shows `replica_version` equal to Inventory's `catalog_version`, and all three consumer groups are assigned (SC-009).

## 3. Smoke the public contract

Use the unchanged Load Client for 20 s, or the contract suite:

```bash
SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh contract
```

Expected: every contract test passes (SC-001), and receipts carry the prices captured at scan time.

## 4. Automated suites

```bash
./build-tests.sh
KAFKA_TEST_BOOTSTRAP=127.0.0.1:19092 KAFKA_TEST_CONTAINER=cs6510-kafka STRICT_LIVE_GATES=1 ./run-tests.sh
```

Expected: every suite passes, including:
- `architecture` (no synchronous service-to-service calls, outbox-only publishing, diagram consistency);
- `messaging` (schemas, compatibility, ordering, parking, broker restarts, direct restoration);
- `integration` (Saga, faults, catalog, analytics, isolation);
- `performance` (statement budgets, held resources).

The direct restoration results are reported separately (SC-016).

## 5. Scenarios

| # | Scenario | Steps | Expected outcome |
|---|---|---|---|
| A | Saga success and insufficient stock | Complete a two-SKU basket; then complete a basket exceeding the stock of one SKU | 200 with exact stock reduction; then 409 `INSUFFICIENT_STOCK`, no stock change, basket OPEN (US1) |
| B | Broker outage | `docker compose -f deploy/kafka/compose.yaml stop`; Start, Scan, Lookup, `/items`, low stock, popular items; one Complete; restart Transaction; start the broker | All succeed except Complete, which returns 500 within 10 s; after the broker returns, the attempt settles and every scan is counted once (SC-004) |
| C | Broker restart under load | Run the 60 s default client; `docker restart cs6510-kafka` at 20 s and `docker kill` plus start at 40 s | Invariants pass after settling; zero lost scans and zero double deductions (SC-005) |
| D | Inventory outage | Stop Inventory; Scan and Complete; restart it | Scans succeed; Completes return 500 and settle after restart (SC-006) |
| E | Analytics outage | Stop Analytics during a run; restart it | No Scan or Complete errors; windows catch up to the highest boundary (SC-007) |
| F | Fault hooks | Run `integration/CheckoutEventFaultTest` and `messaging/ConsumerCrashAfterCommitTest` | Every attempt settles once; no restoration is triggered (SC-003) |
| G | Poison event | Publish an undecodable record to `selfcheckout.checkout.v1` with the test tool `tests/support/PublishRawRecord` | Parked in Inventory's `dead_letter_event` within about 1 s, visible in the status line; other partitions keep flowing |
| H | Catalog replay | Reset Transaction only, then restart it | The replica is rebuilt from the compacted topic with no call to Inventory (US4) |

Record the outcomes in `load-client/reports/event-based-architecture/quickstart-validation.md`.

## 6. Official evidence

```powershell
powershell -File server/benchmark-event-based.ps1 -Workload default -OutputLabel cp4-default-1
powershell -File server/benchmark-event-based.ps1 -Workload stress  -OutputLabel cp4-stress-1
```

Run these on an idle machine with the shared-machine settings from feature 005.

Expected for each run:
- An `evidence.json` file with `comparable=true`, a passing validator, zero parked events, and zero final consumer lag.
- The default run shows zero errors and zero background completions (SC-013).

Then copy the designated runs to `load-client/reports/event-based-architecture/default-report.json` and `stress-report.json`, and write the comparison `README.md` (SC-014).

## 7. Architecture diagram

Open `docs/architecture/event-based-architecture.md` on GitHub. Expected: the diagram renders and shows the gateway, three services, three schemas, the broker, and four topics with their producers and consumer groups (SC-015). `ArchitectureDiagramConsistencyTest` passes.
