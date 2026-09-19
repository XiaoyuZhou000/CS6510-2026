# Self-Checkout Monolithic Server

This directory contains the Java 21 monolithic server for the self-checkout API. It uses the JDK HTTP server, JDBC, and the vendored jars in `lib/`; no Maven or Gradle installation is required.

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

After every database reset, stop any running server and start a fresh server process. This restart is required: `CatalogCache` and `AnalyticsRecorder` load their state only once at server startup, so resetting MySQL alone does not refresh the already-running process's in-memory catalog, open baskets, scan counter, or analytics ring buffer.

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

HTTP contract and integration tests that need a live server skip themselves when the configured server is unavailable. Override endpoints and database credentials with JVM properties such as `-DSERVER_BASE_URL=...`, `-DDB_URL=...`, `-DDB_USER=...`, and `-DDB_PASS=...`.

`ResetBaselineTest` is intentionally opt-in because it drops and recreates the configured database. Run it only against a disposable assignment database:

```bash
./run-tests.sh -DRUN_RESET_BASELINE_TEST=true \
  -DDB_URL='jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC'
```

The test executes `db/init.sql`, starts a fresh server on an available local port, verifies that `/items` contains exactly 2,000 unique SKUs, and verifies that all 2,000 inventory rows contain exactly 10,000 units.
