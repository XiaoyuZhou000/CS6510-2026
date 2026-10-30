# Broker and Service Deployment Contract

**Requirements**: FR-001, FR-007, FR-022, FR-046, FR-056–FR-063 | **Decisions**: D1, D2, D13, D15, D16 | **Base**: [005 service-deployment.md](../../005-microservices-architecture/contracts/service-deployment.md), which stays in force except as changed here

## Units

| Unit | Build output or artifact | Entry point | Ports | Storage |
|---|---|---|---|---|
| Transaction | `server/out/transaction.jar` | `transaction.TransactionMain` | HTTP 8081 | `cs6510_transaction` |
| Inventory | `server/out/inventory.jar` | `inventory.InventoryMain` | HTTP 8082 (the gRPC port 9092 is removed) | `cs6510_inventory` |
| Analytics | `server/out/analytics.jar` | `analytics.AnalyticsMain` | HTTP 8083 (the gRPC port 9093 is removed) | `cs6510_analytics` |
| Broker | `deploy/kafka/compose.yaml`, image `apache/kafka:4.1.0`, container `cs6510-kafka` | Docker Compose | 19092 (host listener, bound to 127.0.0.1) | Named volume `cs6510-kafka-data` |
| Gateway | `deploy/nginx.conf` (unchanged) | Nginx | 8080 | none |

- **Runtime classpath** of each service: its jar, `out/common.jar`, `out/contracts.jar` (generated event classes), `lib/messaging/*` (`kafka-clients` and its dependencies, `protobuf-java`, `slf4j-simple`), and Connector/J. No gRPC jar remains.
- **Startup independence**: any service starts while its peers or the broker are down. The Kafka clients connect and reconnect in the background, and no request thread blocks on them for more than `max.block.ms` (1 s).

## Broker: install, start, stop, reset

Docker Desktop must be running. All commands run from the repository root.

| Action | Command | Effect |
|---|---|---|
| Start | `docker compose -f deploy/kafka/compose.yaml up -d`, then `deploy/kafka/create-topics.ps1` | Starts one KRaft node. Creates the four topics idempotently with the partitions and configuration of the [event catalog](event-catalog.md). Waits until the broker reports them. |
| Stop or restart | `docker compose -f deploy/kafka/compose.yaml stop` / `restart` | Data persists in the volume |
| Status | `deploy/kafka/status.ps1` | Broker reachability, topic partitions, end offsets, and per-group lag (`kafka-consumer-groups.sh --describe` in the container) |
| Reset topics | `deploy/kafka/reset-topics.ps1` | Refuses to run if a service port (8081–8083) answers. Deletes and recreates the four topics, then verifies that their end offsets are 0 and that no committed group offsets remain. |
| Full removal | `docker compose -f deploy/kafka/compose.yaml down -v` | Removes the container and its volume |

**Broker configuration** (environment variables in `compose.yaml`):
- `KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`, a controller quorum of node 1 on the container's own port.
- Listeners: `PLAINTEXT` for the host on `127.0.0.1:19092` and `CONTROLLER` internal.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, `KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE=false`.
- `KAFKA_LOG_FLUSH_INTERVAL_MESSAGES` is set only when `KAFKA_FLUSH_EVERY_MESSAGE=true` (D1, D15).

The exact file is written at implementation and recorded in the broker README `deploy/kafka/README.md`, which also documents Docker Desktop prerequisites and the WSL 2 memory limit.

## Build

```bash
cd server
./build.sh                 # contracts (Maven: event classes + Kafka client) → common → transaction, inventory, analytics
./build.sh transaction     # contracts → common → transaction only
```

`build.sh` deletes `lib/grpc/` and repopulates `lib/messaging/` from Maven (D2). `MAVEN_OFFLINE=1` works once the module has been built.

## Configuration (changes)

| Variable | Applies to | Default |
|---|---|---|
| `KAFKA_BOOTSTRAP` | all | `127.0.0.1:19092` |
| `KAFKA_TOPIC_PREFIX` | all | empty. Tests set a per-run prefix. |
| `CHECKOUT_CONSUMER_THREADS` | Transaction (outcome group), Inventory (checkout group) | 8 |
| `CHECKOUT_RESPONSE_BUDGET_MS` | Transaction | 8000 (unchanged) |
| `CHECKOUT_REPUBLISH_AFTER_MS`, `CHECKOUT_REPUBLISH_MAX_MS` | Transaction | 2000, 5000 |
| `CHECKOUT_STUCK_ALARM_SECONDS` | Transaction | 60 |
| `CHECKOUT_RECOVERY_SCAN_SECONDS`, `CHECKOUT_WAIT_BACKSTOP_MS` | Transaction | 30, 1000 (unchanged) |
| `OUTBOX_SWEEP_MS`, `OUTBOX_MARK_BATCH_MS` | Transaction, Inventory | 1000, 20 |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_COALESCE_MS` | Transaction (scan relay) | 500, 0 (unchanged) |
| `CONSUMER_MAX_ATTEMPTS` | all consumers | 5 |
| `SCAN_PRODUCER_LINGER_MS` | Transaction | 5 |
| `KAFKA_FLUSH_EVERY_MESSAGE` | broker (compose) | `false` |

**Removed**: `INVENTORY_GRPC_PORT`, `ANALYTICS_GRPC_PORT`, `INVENTORY_GRPC_TARGET`, `ANALYTICS_GRPC_TARGET`, `INVENTORY_RPC_DEADLINE_MS`, `ANALYTICS_RPC_DEADLINE_MS`, and `CATALOG_REFRESH_SECONDS`. No service reads a peer's host or port, and `MicroserviceBoundaryTest` checks this.

Database configuration, pools, JDBC session settings, credentials, and the 122-connection budget are unchanged.

## Start, stop, restart

```bash
docker compose -f deploy/kafka/compose.yaml up -d && powershell -File deploy/kafka/create-topics.ps1   # once per boot
cd server
./run-inventory.sh      # publishes the catalog at startup (D9)
./run-transaction.sh
./run-analytics.sh
```

Each service shuts down in this order: HTTP, then consumers (with a final `commitSync` of processed offsets), then the dispatcher (with a final mark), then background workers, then the producer, then the pool.

## Baseline reset (D13, FR-059)

`db/reset-baseline.ps1` performs these steps:
1. Refuses to run while ports 8081–8083 answer.
2. Runs `deploy/kafka/reset-topics.ps1`.
3. Runs `db/reset-all.ps1`, which uses per-service credentials and also empties each service's `outbox_event`, `dead_letter_event`, and `consumer_offset`.
4. Prints the baseline: 2,000 / 2,000 / 20,000,000; zero transactions, scans, outbox rows, and parked events; empty topics.

The benchmark harness then starts the services and waits for readiness (below).

## Operational status (FR-046)

Each service logs its status line every `STATUS_INTERVAL_SECONDS` (30), extended with:

| Service | New fields |
|---|---|
| Transaction | `unpublished_outbox`, `unpublished_scans` (formerly `undelivered_scans`), `parked_events`, `stuck_attempts`, `replica_version`, `outcome_consumer_lag` |
| Inventory | `unpublished_outbox`, `parked_events`, `catalog_version`, `catalog_published_at`, `checkout_consumer_lag` |
| Analytics | `parked_events`, `consumer_next_offset`, `scan_consumer_lag` |

**Pending work is zero when**: there are no pending attempts, no unpublished outbox rows or scans in any service, consumer lag is 0 for all three groups, and Analytics' checkpoint equals its highest eligible boundary. Parked events are reported separately. A benchmark run with any parked event is not comparable.

**Readiness, before the Load Client starts**: Transaction's `replica_version` equals Inventory's `catalog_version` (SC-009), and every consumer group has its partitions assigned.

## Benchmark harness

`server/benchmark-event-based.ps1` replaces `benchmark-microservices.ps1`, keeping its steps and adding the broker:
1. Checks that ports 8080–8083 are free and that the broker is up with its topics.
2. Runs `db/reset-baseline.ps1`.
3. Starts Inventory, Transaction, and Analytics with `PERF_METRICS=true`, plus the gateway, and waits for readiness.
4. Runs the unchanged Load Client with exactly `java -cp out Main`, or `java -cp out Main --stations=100 --duration=120`.
5. Samples every 2 s: 005's samples plus consumer lag per group, unpublished outbox rows, and parked events.
6. Waits for zero pending work, then runs `db/validate-invariants.ps1`.
7. Records the MySQL durability settings, the broker image and flush setting, and the Docker Desktop version.
8. Writes `load-client/reports/event-based-architecture/<label>/`.

`server/benchmark-compare.ps1` keeps its interleaved A/B method. Its baseline variant runs feature 005 from a worktree of `8891c57` with that worktree's own `benchmark-microservices.ps1`. It keeps `-IdleCpuCores` and `-BlockingProcess` for this shared machine.

**Official evidence** (D16, FR-062): the first CP4 run of each workload, designated in advance, is copied byte-identically to `load-client/reports/event-based-architecture/default-report.json` and `stress-report.json`. A `README.md` beside them gives the commands, the baseline, the FR-061 counts, parked events (0), and the comparison with `load-client/reports/microservices-architecture/`.

## Architecture diagram (FR-063, D16)

`docs/architecture/event-based-architecture.md` contains the Mermaid diagram, which GitHub renders, and links `event-based-architecture.svg`, exported with Mermaid CLI at a pinned version. The root `README.md` links both. The diagram shows:
- Nginx (8080);
- the three services with their HTTP ports and schemas;
- the Kafka broker with the four topics, each with its producer arrow and its consumer-group arrow;
- each service's outbox and dead-letter tables;
- no direct edge between services.
