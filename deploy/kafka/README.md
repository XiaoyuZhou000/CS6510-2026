# Message Broker: Apache Kafka 4.1.0

The self-checkout services communicate only through domain events carried by this one broker (constitution v6.0.0, Principles III and VIII). It is a single Kafka 4.1.0 node in KRaft mode, combined broker and controller, run natively on Windows as one JVM. The choice and its alternatives are in [research D1](../../specs/006-event-based-architecture/research.md), and the topics are in the [event catalog](../../specs/006-event-based-architecture/contracts/event-catalog.md#topics).

The broker needs no Docker. Docker Desktop cannot run on the benchmark machine, because its hypervisor conflicts with other software there.

## Install

1. Download the official binary release `kafka_2.13-4.1.0.tgz` from `https://archive.apache.org/dist/kafka/4.1.0/` and check it against `kafka_2.13-4.1.0.tgz.sha512` from the same folder.
2. Unpack it into a folder whose path has no spaces, for example `C:\tools\kafka_2.13-4.1.0`.
3. Set `KAFKA_HOME` to that folder in the shell that runs the scripts, the benchmark harness, or the live tests.
4. The broker runs on the JDK from `JAVA_HOME`, or else the `java` on `PATH`. JDK 21 is used here.

Every command below runs from the repository root in Windows PowerShell 5.1 or PowerShell 7.

## Files

| File | Purpose |
|---|---|
| `server.properties` | The committed broker configuration. `start-broker.ps1` adds `log.dirs` and the optional flush setting. |
| `start-broker.ps1` | Writes the effective configuration, formats the log on first use, starts the broker, and creates the topics |
| `stop-broker.ps1` | Stops the broker |
| `create-topics.ps1` | Waits for the broker, creates the four topics idempotently, and verifies them |
| `status.ps1` | Broker reachability, topic partitions and end offsets, and consumer-group lag (`-Json` for the benchmark harness) |
| `reset-topics.ps1` | Empties the broker for a baseline |
| `kafka-tools.ps1` | Helpers dot-sourced by the scripts above |

## Operation

| Action | Command | Effect |
|---|---|---|
| Start | `./deploy/kafka/start-broker.ps1` | Starts the broker as a hidden background JVM, waits until it answers on `127.0.0.1:19092`, and runs `create-topics.ps1`. `-NoTopics` skips the topic step. If the broker already runs, only the topic step runs. |
| Stop | `./deploy/kafka/stop-broker.ps1` | Ends the broker JVM. Windows has no signal that asks a background JVM to shut down cleanly, so this is always a forced stop, the same as a crash. |
| Restart | `stop-broker.ps1`, then `start-broker.ps1` | The log, the topics, and the committed offsets survive |
| Status | `./deploy/kafka/status.ps1` (or `-Json`) | Prints reachability, each topic's partitions and end offsets, and the lag of `inventory-checkout`, `transaction-checkout-outcome`, and `analytics-scan-ingest`. A group that does not exist yet is reported as `absent`. It takes about 4 s, because each Kafka tool starts its own JVM. |
| Create topics | `./deploy/kafka/create-topics.ps1 [-TopicPrefix <p>]` | Idempotent. `-TopicPrefix` matches the services' `KAFKA_TOPIC_PREFIX`. Exits nonzero if an existing topic differs from the catalog. |
| Reset | `./deploy/kafka/reset-topics.ps1` | Refuses to run while any of ports 8081–8083 answers. Stops the broker, deletes its log directory, and starts it again, which reformats the log and recreates the four topics. Then it verifies that every end offset is 0 and that no consumer group remains. `db/reset-baseline.ps1` calls it. |
| Full removal | `stop-broker.ps1`, then delete `KAFKA_DATA_DIR` | Removes every record |

## State directory

`KAFKA_DATA_DIR`, by default `<KAFKA_HOME>\cs6510-data`, holds:
- `kraft-logs\`: the broker's log directory (`log.dirs`), with every topic, the committed consumer offsets, and the cluster metadata. It is formatted once, with the fixed cluster ID `Y3M2NTEwLWthZmthLTAwMQ`.
- `app-logs\`: the broker's own logs and its console output (`broker.out`, `broker.err`).
- `server.properties`: the effective configuration, rewritten at every start.
- `broker.pid`: the process ID of the last start.

## Listeners

- **Clients**: `PLAINTEXT` on `127.0.0.1:19092`. Services read it from `KAFKA_BOOTSTRAP`, default `127.0.0.1:19092`.
- **Controller**: `CONTROLLER` on `127.0.0.1:19093`, with the single voter `1@127.0.0.1:19093`.
- Both are bound to loopback only, and both lie above the machine's dynamic port range (1024–15000), so an outbound connection never holds them.

## Configuration

| Setting (`server.properties`) | Value | Why |
|---|---|---|
| `process.roles`, `node.id` | `broker,controller`, `1` | One combined KRaft node |
| `offsets.topic.replication.factor`, `transaction.state.log.replication.factor`, `min.insync.replicas` | `1` | Single node |
| `auto.create.topics.enable` | `false` | Topics exist only as documented in the event catalog |
| `unclean.leader.election.enable` | `false` | No acknowledged write is discarded |
| `group.initial.rebalance.delay.ms` | `0` | A starting consumer group gets its partitions without a 3 s delay |
| `log.segment.bytes`, `offsets.topic.segment.bytes`, `log.roll.ms` | 1 GiB, 1 GiB, `Long.MAX_VALUE` | Windows: segments never roll (below) |
| `log.retention.ms`, `log.retention.bytes` | `-1`, `-1` | Windows: nothing is deleted by retention |
| `delete.topic.enable` | `false` | Windows: a topic deletion would stop the broker |
| `log.flush.interval.messages` | Added as `1` only when `KAFKA_FLUSH_EVERY_MESSAGE=true` | See *Durability* |

Clients set the rest (research D1): producers use `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `delivery.timeout.ms=30000`, and `max.block.ms=1000`. Consumers use `enable.auto.commit=false`, `auto.offset.reset=earliest`, and `isolation.level=read_uncommitted`, and commit offsets only after their local database commit. `KAFKA_HEAP_OPTS` overrides the broker's default heap of `-Xmx1G -Xms1G`.

## Topics

| Topic | Partitions | Cleanup and retention | Record key |
|---|---:|---|---|
| `selfcheckout.catalog.v1` | 1 | `compact`, `retention.ms=-1` | SKU, or `__snapshot__` |
| `selfcheckout.checkout.v1` | 16 | `delete`, 7 days (`604800000`) | Transaction ID |
| `selfcheckout.checkout-outcome.v1` | 16 | `delete`, 7 days | Transaction ID |
| `selfcheckout.scan-accepted.v1` | 1 | `delete`, 7 days | Transaction ID |

Every topic also pins `segment.bytes=1073741824` and `segment.ms=9223372036854775807`. Topics are created only by `create-topics.ps1`, and `auto.create.topics.enable=false` makes a misspelled topic fail instead of appearing silently.

## Windows constraints

Kafka does not officially support Windows. A spike on 2026-10-29 showed what works and what does not:
- **Works**: starting the broker, producing and consuming, committed group offsets, a compacted topic's writes, and recovery after a forced kill.
- **Fails**: Kafka cannot rename or delete a log file that it still has open. Deleting a topic renames its open partition directory, fails with `AccessDeniedException`, and takes the whole log directory offline, which stops the broker. Deleting old segments by retention and rewriting them by compaction fail the same way.
- **Also fails**: the `bin\windows\*.bat` wrappers stop with "input line too long" while building the classpath. The scripts here run the tools as their Java main classes instead.

So segments never roll, and topics are never deleted, which `delete.topic.enable=false` enforces. Retention and compaction therefore never act, and every topic keeps every record until the next reset. That is harmless here:
- every benchmark starts from a reset;
- a 1 GiB segment holds far more than one run;
- the catalog consumer replays the topic from offset 0 and keeps the last value per SKU, with or without compaction.

Tests create topics with a per-run prefix and never delete them, so those topics accumulate until the next reset.

## Durability and `KAFKA_FLUSH_EVERY_MESSAGE`

- **Confirmed records**: a record is in the broker's log before `acks=all` returns. By default the log reaches disk through the operating system's file cache, not an fsync per message.
- **Broker stops and crashes**: a forced stop or a crash of the broker process keeps every acknowledged record, because the written pages belong to the operating system, not to the process. The next start recovers the unflushed tail of each segment.
- **Residual risk**: losing the host itself, such as a power loss or an OS crash, before the file cache is flushed can lose recently confirmed records. That is outside the constitution's broker-restart guarantee.
- **Option**: to remove that risk, start the broker with the flush option, which writes `log.flush.interval.messages=1` (an fsync per message). Its cost is measured at checkpoint CP3 (research D15).

  ```powershell
  ./deploy/kafka/stop-broker.ps1
  $env:KAFKA_FLUSH_EVERY_MESSAGE = 'true'
  ./deploy/kafka/start-broker.ps1
  ```

  Clear the variable and restart the broker to return to the default.
- **No business state is lost either way**: every event is first recorded in its producer's database outbox and marked published only after confirmation (research D5). An unconfirmed record is re-sent from there.

## Rejected alternatives (research D1)

- **Kafka in Docker**: the first plan. Rejected because Docker Desktop needs the Windows hypervisor, which must stay off on the benchmark machine.
- **Kafka on Windows with normal retention and compaction**: the first segment deletion or compaction would stop the broker.
- **RabbitMQ 4 with quorum queues**: durable, with built-in dead-lettering. Rejected because it needs Erlang. Per-key ordering with competing consumers needs extra exchange or queue modes, and it has no replayable log, so the catalog would need a snapshot-request event.
- **ActiveMQ Artemis 2.x**: runs natively on Windows, with message groups and dead-letter addresses. Rejected because it has no replayable log, and a durable send waits for a journal sync, adding a commit-sized cost to every hop.
- **A second broker, or a broker plus a cache**: forbidden by constitution Principle VIII.
