# Contract: Deployment, Baseline, Harness, and Evidence

Extends `specs/006-event-based-architecture/contracts/broker-and-deployment.md`. See research N14–N15.

## Units

| Unit | Build | Run | Port | Database | Kafka |
|---|---|---|---|---|---|
| Transaction | `server/build.sh transaction` | `server/run-transaction.sh` | 8081 | `cs6510_transaction` | produces `SaleCompleted` (new) |
| Inventory | unchanged | unchanged | 8082 | unchanged | unchanged |
| Analytics | unchanged | unchanged | 8083 | unchanged | unchanged |
| **Notification** | `server/build.sh notification` | `server/run-notification.sh` | 8084 | `cs6510_notification` (user `cs6510_notification_svc`) | consumes sale-completed and catalog; produces channel topics |
| Gateway | `deploy/nginx.conf` (adds `/notifications/`) | unchanged | 8080 | — | — |
| Broker | `deploy/kafka/` (create-topics adds the sale topic) | unchanged | 19092 | — | 5 fixed topics + 1 per subscriber |
| Subscriber test application | `subscriber-client/build.sh` | `subscriber-client/run.sh SubscriberMain …` | — | files in the run directory | consumes channel topics |

## Notification configuration

| Variable | Default | Meaning |
|---|---|---|
| `NOTIFICATION_HTTP_PORT` | 8084 | |
| `NOTIFICATION_DB_URL` | `jdbc:mysql://127.0.0.1:3307/cs6510_notification` with 006's flags | |
| `NOTIFICATION_DB_USER` and `NOTIFICATION_DB_PASSWORD` | `cs6510_notification_svc` and the user-level environment variable | The password is never printed |
| `NOTIFICATION_DB_POOL_SIZE` and `NOTIFICATION_DB_POOL_MAX_SIZE` | 12 and 20 | The connection budget becomes 64 + 48 + 10 + 20 = 142 |
| `KAFKA_BOOTSTRAP` and `KAFKA_TOPIC_PREFIX` | as in 006 | |
| `NOTIFICATION_SALE_CONSUMER_THREADS` | 4 | |
| `NOTIFICATION_LANE_IN_FLIGHT` | 64 | |
| `NOTIFICATION_PARK_AFTER_CHARGED` | 8 | |
| `NOTIFICATION_SWEEP_MS` | 1000 | |
| `NOTIFICATION_TEST_FAULTS` | unset | `1` enables `POST /notifications/ops/faults` |

## Baseline (`db/reset-baseline.ps1`, extended)

1. Refuses to run while 8081–8084 answer.
2. Resets the four schemas through `reset-service.ps1`: Inventory reseeded with 2,000 × 10,000; Transaction and Analytics as in 006; Notification with `subscription_clock.version = 0` and no other rows.
3. Runs `deploy/kafka/reset-topics.ps1`, which replaces the log directory (removing every channel topic) and creates the five fixed topics.
4. With `-RunDir`, removes that run directory.
5. Prints the baseline, now including the Notification counts (all zero) and the topic list.

## Status (`deploy/kafka/status.ps1`, the service status lines)

The `notification-sale-completed` group lag is added. Notification's status line gives the `ops/status` fields every 10 s.

## Harness (`server/benchmark-notification.ps1`)

```text
-Subscribers 100 -Seed 20261111 -SkusPerSubscriber 20 -Workload default|stress
-Fault none|reject|stall -FaultSubscriber sub-0001 -RunName <name> -DrainSeconds 120
```

1. Checks that ports 8080–8084 are free and the broker is up; the idle gate is as in 006.
2. Runs the baseline with `-RunDir`.
3. Starts the four services and Nginx, waiting until each answers.
4. Starts `SubscriberMain` and waits for `ready.json`.
5. With a fault, sets `notification.channel.<fault>:<FaultSubscriber>` through `ops/faults`.
6. Runs the unchanged Load Client (`java -cp out Main` or `--stations=100 --duration=120`), sampling backlog every 2 s.
7. Waits for the drain, at most `-DrainSeconds`. With a fault, it then clears the fault, retries parked notifications, and waits for the drain again.
8. Stops the services and the subscriber application; runs feature 006's `validate-invariants.ps1`, `export-notification-evidence.ps1`, and `ReconcileMain --mode=fixed`.
9. Writes `notification-report.json` and `.md`, and copies the Load Client report to `load-client-report.json` unmodified.

## Evidence layout (`load-client/reports/notification-service/`)

```text
README.md                         # index, comparison with event-based-architecture reports, SC table
cp0/ cp1/ cp2/ cp3-*/             # checkpoint runs
scaling/{10,25,50,100}/           # default workload; notification-report.*, reconciliation.json, load-client-report.json
default-report.json               # official default Load Client report (unmodified), 100 subscribers
default-notification-report.json  # and .md, reconciliation.json
stress-report.json                # official stress Load Client report (unmodified), 100 subscribers
stress-notification-report.json   # and .md, reconciliation.json
isolation/{healthy,reject,stall}-{1,2,3}/   # SC-006 runs; README gives the medians
change-scenario/                  # scenario E: changes.jsonl, reconciliation.json (history mode)
```

## Architecture diagram

`docs/architecture/notification-service.md` (Mermaid, rendered by GitHub) and `docs/architecture/notification-service.svg` show:
- the gateway;
- the four services, each with its database;
- Kafka with its five fixed topics and the per-subscriber channel family, each with its producer and consumer arrows;
- the subscriber test application with N subscriber threads.

`ArchitectureDiagramConsistencyTest` checks the diagram against `TopicNames` and `EventTypes`.
