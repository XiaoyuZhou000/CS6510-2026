# Message Broker: Apache Kafka 4.1.0

The self-checkout services communicate only through domain events carried by this one broker (constitution v6.0.0, Principles III and VIII). It is a single Kafka 4.1.0 node in KRaft mode, combined broker and controller, running in Docker from the official `apache/kafka:4.1.0` image. The choice and its alternatives are in [research D1](../../specs/006-event-based-architecture/research.md), and the topics are in the [event catalog](../../specs/006-event-based-architecture/contracts/event-catalog.md#topics).

Only the broker runs in a container. The services stay plain JARs on the host.

## Prerequisites

- **Docker Desktop** for Windows with the **WSL 2** backend, running before any command below. On Unix, any Docker Engine with the Compose plugin works.
- **WSL 2 memory limit**: the broker needs about 1 GB. Docker Desktop's VM takes memory from the WSL 2 limit, which defaults to half of the host's RAM. To cap it, set `memory=` under `[wsl2]` in `%UserProfile%\.wslconfig`, for example `memory=4GB`, then run `wsl --shutdown` and restart Docker Desktop.
- **Windows PowerShell 5.1** or PowerShell 7 for the scripts. Every command runs from the repository root.
- **Shared machine**: Docker Desktop conflicts with some games on the benchmark machine. Start it only for broker work, and stop it afterwards (*Stop* below).

## Files

| File | Purpose |
|---|---|
| `compose.yaml` | The container `cs6510-kafka`, its listener, its volume, and its configuration |
| `create-topics.ps1` | Waits for the broker, creates the four topics idempotently, and verifies them |
| `status.ps1` | Broker reachability, topic partitions and end offsets, and consumer-group lag (`-Json` for the benchmark harness) |
| `reset-topics.ps1` | Deletes and recreates the four topics for a baseline, then verifies they are empty |
| `kafka-tools.ps1` | Helpers dot-sourced by the scripts above |

## Operation

| Action | Command | Effect |
|---|---|---|
| Start | `docker compose -f deploy/kafka/compose.yaml up -d`, then `./deploy/kafka/create-topics.ps1` | Starts the node and creates any missing topic. Exits nonzero if an existing topic's partitions or configuration differ from the catalog. |
| Stop | `docker compose -f deploy/kafka/compose.yaml stop` | Stops the container. Data stays in the volume. |
| Restart | `docker compose -f deploy/kafka/compose.yaml restart` | Restarts the Kafka process. The log, the topics, and the committed offsets survive. |
| Status | `./deploy/kafka/status.ps1` (or `-Json`) | Prints reachability, each topic's partitions and end offsets, and the lag of `inventory-checkout`, `transaction-checkout-outcome`, and `analytics-scan-ingest`. A group that does not exist yet is reported as `absent`. |
| Create topics | `./deploy/kafka/create-topics.ps1 [-TopicPrefix <p>]` | Idempotent. `-TopicPrefix` matches the services' `KAFKA_TOPIC_PREFIX`. |
| Reset topics | `./deploy/kafka/reset-topics.ps1` | Refuses to run while any of ports 8081–8083 answers. Deletes and recreates the four topics, then verifies that every end offset is 0 and that no group holds a committed offset. It never restarts the container. `db/reset-baseline.ps1` calls it. |
| Full removal | `docker compose -f deploy/kafka/compose.yaml down -v` | Removes the container and the volume `cs6510-kafka-data`, deleting every record. |
| Stop Docker Desktop | Quit it from the tray icon, or `docker desktop stop` (Docker Desktop 4.37 and later) | Stops Docker's VM after broker work on the shared machine |

## Topics

| Topic | Partitions | Cleanup and retention | Record key |
|---|---:|---|---|
| `selfcheckout.catalog.v1` | 1 | `compact`, `retention.ms=-1` | SKU, or `__snapshot__` |
| `selfcheckout.checkout.v1` | 16 | `delete`, 7 days (`604800000`) | Transaction ID |
| `selfcheckout.checkout-outcome.v1` | 16 | `delete`, 7 days | Transaction ID |
| `selfcheckout.scan-accepted.v1` | 1 | `delete`, 7 days | Transaction ID |

Topics are created only by `create-topics.ps1`; `auto.create.topics.enable=false`, so a misspelled topic fails instead of appearing silently.

## Listener and volume

- **Host listener**: `PLAINTEXT` on `127.0.0.1:19092`, published only on loopback. The port lies above the machine's dynamic port range (1024–15000), so an outbound connection never holds it. Services read it from `KAFKA_BOOTSTRAP`, default `127.0.0.1:19092`.
- **Controller listener**: `CONTROLLER` on the container's internal port 9093, which is not published. The quorum is the single voter `1@localhost:9093`.
- **Tools inside the container** reach the broker at `localhost:19092`, which is how the scripts run `kafka-topics.sh`, `kafka-get-offsets.sh`, and `kafka-consumer-groups.sh` through `docker exec cs6510-kafka`.
- **Data**: the named volume `cs6510-kafka-data`, mounted at the log directory `/var/lib/kafka/data`. A fixed `CLUSTER_ID` keeps the formatted log directory valid when the container is re-created.

## Configuration

| Setting (`compose.yaml`) | Value | Why |
|---|---|---|
| `KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES` | `1`, `broker,controller` | One combined KRaft node |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | `1`, `1` | Single node; replication factor and `min.insync.replicas` are 1 |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` | Topics exist only as documented in the event catalog |
| `KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE` | `false` | No acknowledged write is discarded |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | `0` | A starting consumer group gets its partitions without a 3 s delay |
| `KAFKA_LOG_FLUSH_INTERVAL_MESSAGES` | Set to `1` only when `KAFKA_FLUSH_EVERY_MESSAGE=true` | See *Durability* |

Clients set the rest (research D1): producers use `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `delivery.timeout.ms=30000`, and `max.block.ms=1000`. Consumers use `enable.auto.commit=false`, `auto.offset.reset=earliest`, and `isolation.level=read_uncommitted`, and commit offsets only after their local database commit.

## Durability and `KAFKA_FLUSH_EVERY_MESSAGE`

- **Confirmed records**: a record is in the broker's log before `acks=all` returns. By default the log reaches disk through the operating system's page cache, not an fsync per message.
- **Broker restarts**: a clean or crash restart of the Kafka process or its container (`docker restart`, `docker kill`) keeps the log, because the page cache belongs to Docker's VM, which keeps running.
- **Residual risk**: losing the whole VM or host before the page cache is flushed can lose recently confirmed records. That is outside the constitution's broker-restart guarantee.
- **Option**: to remove that risk, start the broker with the flush option, which sets `log.flush.interval.messages=1` (an fsync per message). Its cost is measured at checkpoint CP3 (research D15).

  ```powershell
  $env:KAFKA_FLUSH_EVERY_MESSAGE = 'true'
  docker compose -f deploy/kafka/compose.yaml up -d --force-recreate
  ```

  Remove the variable and recreate the container to return to the default. Compose passes the value to the container as `CS6510_FLUSH_EVERY_MESSAGE`, because the image turns every `KAFKA_*` variable into a broker property.
- **No business state is lost either way**: every event is first recorded in its producer's database outbox and is marked published only after confirmation (research D5). An unconfirmed record is re-sent from there.

## Rejected alternatives (research D1)

- **Kafka on Windows without Docker**: Kafka does not support Windows. Log-segment deletion and compaction fail there on file locks, which would break the compacted catalog topic.
- **RabbitMQ 4 with quorum queues**: durable, with built-in dead-lettering. Rejected because it needs Erlang or Docker anyway. Per-key ordering with competing consumers needs extra exchange or queue modes, and it has no replayable log, so the catalog would need a snapshot-request event.
- **ActiveMQ Artemis 2.x**: runs natively on Windows, with message groups and dead-letter addresses. Rejected because it has no replayable log, and a durable send waits for a journal sync, adding a commit-sized cost to every hop.
- **A second broker, or a broker plus a cache**: forbidden by constitution Principle VIII.
