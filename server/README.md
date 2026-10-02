# Self-Checkout Layered Server

This directory contains the Java 21 layered server for the self-checkout API. It uses the JDK HTTP server, JDBC, and the vendored jars in `lib/`; no Maven or Gradle installation is required.

## Package layout

Production code under `src/` has one owner per package:

- `api/` — HTTP routing, validation, JSON translation, and the `api.Main` composition root
- `transaction/` — catalog views, basket lifecycle, scans, completion, and inventory query policy
- `analytics/` — the accepted-scan pipeline, lifecycle, structured operational logging, and
  committed popular-item queries
- `database/` — store contracts, connection pooling, JDBC queries, and atomic persistence

Tests under `tests/` are grouped into `architecture/`, `unit/`, `contract/`, `database/`,
`integration/`, and shared `support/`. Both build scripts discover Java files recursively, compile
production classes to `out/main`, and compile tests to `out/test`.

## Analytics pipeline

Accepted scans move through three independently executing filters in a fixed order:

```text
AnalyticsService -> Window Filter -> Ranking Filter -> Persistence Filter -> PopularWindowStore
```

- The **Window Filter** assigns the single analytics order, owns the 1,000-scan ring, and emits
  immutable complete snapshots at `[1,1000]`, `[501,1500]`, and each later 500-event hop.
- The **Ranking Filter** counts a snapshot, sorts by count descending then SKU ascending, and emits
  at most ten contiguous ranks.
- The **Persistence Filter** commits ranked windows atomically and in order. A failed oldest window
  remains in place and retries after 50 ms, 200 ms, and then capped 1-second waits; newer windows
  cannot overtake it.

Each arrow between filters is an unbounded FIFO `LinkedBlockingQueue`; filters never call one
another directly. `AnalyticsService` owns the three named workers, accepts scans under a short
lifecycle lock, and reads results only from committed storage. During shutdown it rejects new
ingestion, places one end marker, drains stage by stage, and joins all workers against one deadline.
Timeout, caller interruption, or an unexpected worker failure interrupts peers and produces a
structured forced/failure outcome rather than reporting a clean drain.

This is an internal decomposition only. `spec/self-checkout-openapi.yaml`, `db/init.sql`, and the
load client remain unchanged. In particular, `GET /analytics/popular-items` keeps the same fields,
limit behavior, empty response, and status mappings, and it never exposes queues or in-flight state.

## Prerequisites

- JDK 21 or newer (`java` and `javac` on `PATH`)
- Bash (Git Bash, WSL, Linux, or macOS) for the `*.sh` scripts
- MySQL 8.0 or newer
- The MySQL command-line client for `db/init-db.ps1`

The examples below assume commands are run from the repository root. The default database endpoint is `127.0.0.1:3307`, the database name is `cs6510_selfcheckout`, and the default database user is `root`.

## Build and run

Build the server:

```bash
cd server
./build.sh
```

Run it with defaults:

```bash
./run.sh
```

`run.sh` accepts these positional arguments, in order:

```text
./run.sh [port] [dbHost] [dbPort] [dbName] [dbUser] [dbPassword]
```

For example:

```bash
./run.sh 8080 127.0.0.1 3307 cs6510_selfcheckout root secret
```

The same settings can be supplied through `SERVER_PORT`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD`. Positional arguments take precedence. `DB_POOL_SIZE` optionally controls the JDBC pool size (default: 10).

| Environment variable | Default | Purpose |
|---|---|---|
| `SERVER_PORT` | `8080` | HTTP listen port |
| `DB_HOST` | `127.0.0.1` | MySQL host |
| `DB_PORT` | `3307` | MySQL port |
| `DB_NAME` | `cs6510_selfcheckout` | MySQL database |
| `DB_USER` | `root` | MySQL user |
| `DB_PASSWORD` | empty | MySQL password |
| `DB_POOL_SIZE` | `10` | JDBC connection-pool size |

To avoid placing a password in shell history, prefer the environment variable:

```powershell
$env:DB_PASSWORD = Read-Host "MySQL password"
bash ./server/run.sh
```

## Repeatable database reset

`db/init-db.ps1` drops and recreates `cs6510_selfcheckout`, then seeds exactly 2,000 catalog items with 10,000 units of stock each. Run it before every load-test run:

```powershell
./db/init-db.ps1 -MySqlUser root -MySqlHost 127.0.0.1 -Port 3307
```

The script securely prompts for the password. You can also pass `-Password` or `-MySqlExe` when needed.

After every database reset, stop any running server and start a fresh server process. This restart is required: `CatalogCache` and `AnalyticsService` load their state only once at server startup, so resetting MySQL alone does not refresh the already-running process's in-memory catalog, open baskets, scan counter, or analytics ring buffer.

The required order for each independent run is therefore:

1. Stop the server.
2. Run `db/init-db.ps1`.
3. Start a fresh server process with `server/run.sh`.
4. Run the unmodified load client and save its JSON report.

For the assignment validation gate, perform the sequence twice, resetting and restarting between runs:

```bash
# Default workload: 10 stations for 60 seconds
cd load-client
./run.sh --stations=10 --duration=60 --reportDir=./reports

# After another database reset and server restart:
./run.sh --stations=100 --duration=120 --reportDir=./reports
```

## Tests

Build and run the JUnit suite:

```bash
cd server
./build.sh
./build-tests.sh
./run-tests.sh
```

HTTP contract and integration tests that need a live server skip themselves when the configured server is unavailable. A skipped test is useful for local unit-only work but is not evidence for the final validation gate. For final validation, start MySQL and the layered server first and confirm the JUnit summary reports no skipped live checks.

Pass test configuration as arguments to `run-tests.sh`:

```bash
./run-tests.sh \
  -DSERVER_BASE_URL=http://localhost:8080 \
  -DDB_URL='jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC' \
  -DDB_USER=root \
  -DDB_PASS=secret
```

`run-tests.sh` also maps the `DB_USER` and `DB_PASSWORD` environment variables to the default
`DB_USER` and `DB_PASS` JVM properties. Explicit `-D` arguments override those defaults.

`ResetBaselineTest` is intentionally opt-in because it drops and recreates the configured database. Run it only against a disposable assignment database:

```bash
./run-tests.sh -DRUN_RESET_BASELINE_TEST=true \
  -DDB_URL='jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC'
```

The test executes `db/init.sql`, starts a fresh server on an available local port, verifies that `/items` contains exactly 2,000 unique SKUs, and verifies that all 2,000 inventory rows contain exactly 10,000 units.
