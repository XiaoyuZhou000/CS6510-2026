# Self-Checkout Distributed Services

Java 21, MySQL 8.0+, Bash, Nginx, and the vendored Connector/J and JUnit JARs are required. The three domain services are separate JVMs over one MySQL database. Nginx provides the unchanged public base URL, `http://localhost:8080`; the load client and contract tests use that URL.

| Service | Private port | Public routes | Artifact / entry point |
|---|---:|---|---|
| Transaction | 8081 | `POST /transactions`, `POST /transactions/{id}/items`, `POST /transactions/{id}/complete`, `GET /transactions/{id}` | `out/transaction.jar`, `transaction.TransactionMain` |
| Inventory | 8082 | `GET /items`, `GET /inventory/low-stock` | `out/inventory.jar`, `inventory.InventoryMain` |
| Analytics | 8083 | `GET /analytics/popular-items` | `out/analytics.jar`, `analytics.AnalyticsMain` |

`common.jar` contains only shared HTTP/JSON and JDBC utilities. Each service JAR contains its own API, business, and database adapters and requires only `common.jar` plus `lib/mysql-connector-j-9.0.0.jar` at runtime. Transaction validates prices and deducts stock directly in the shared database transaction. Analytics polls durable `accepted_scan` rows and checkpoints complete windows; neither service calls a peer process.

## Build and configure

From `server/` with JDK 21 on `PATH`:

```bash
./build.sh                 # all services and common
./build.sh transaction     # common and Transaction only
./build.sh inventory       # common and Inventory only
./build.sh analytics       # common and Analytics only
```

Set the **same** `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` for each JVM. Defaults are `127.0.0.1`, `3307`, `cs6510_selfcheckout`, `root`, and an empty password. `DB_POOL_SIZE` defaults to 32 for Transaction Service and 10 for Inventory and Analytics; set it per process when overriding. Pass passwords by environment variable rather than a command argument. `TRANSACTION_PORT`, `INVENTORY_PORT`, and `ANALYTICS_PORT` override the private defaults; an optional numeric argument to each run script overrides its port. The fixed Nginx map in `deploy/nginx.conf` assumes 8081/8082/8083, so update the map when overriding a private port.

## Start, stop, and restart independently

Run these in three separate Bash terminals from `server/`:

```bash
./run-transaction.sh
./run-inventory.sh
./run-analytics.sh
```

Each script checks for its own JAR, `common.jar`, and Connector/J, then replaces the shell with its JVM. Stop a service with Ctrl+C in **its** terminal and rerun only that script to restart it. The other two JVMs and Nginx stay up. On Windows, use Git Bash with JDK 21 on `PATH`; the scripts choose the Windows `;` classpath separator. The equivalent direct Windows launch, useful for a service manager, is `java -cp "out/transaction.jar;out/common.jar;lib/mysql-connector-j-9.0.0.jar" transaction.TransactionMain` from `server/`, substituting the service JAR and main class for the other two.

Start Nginx from the repository root on Unix:

```bash
mkdir -p logs temp
nginx -p "$(pwd)/" -c deploy/nginx.conf
# later: nginx -p "$(pwd)/" -c deploy/nginx.conf -s stop
```

On Windows, use an extracted official Nginx for Windows distribution. Create an ASCII-only runtime directory with `conf/`, `logs/`, and `temp/`, copy `deploy/nginx.conf` to `conf/nginx.conf`, then run the following PowerShell commands, replacing the two paths with your local paths:

```powershell
$nginx = 'C:\path\to\nginx.exe'
$prefix = 'C:\path\to\nginx-runtime'
& $nginx -p "$prefix/" -c conf/nginx.conf -t
Start-Process -FilePath $nginx -WorkingDirectory $prefix -ArgumentList @('-p',"$prefix/",'-c','conf/nginx.conf') -WindowStyle Hidden
# later: & $nginx -p "$prefix/" -c conf/nginx.conf -s stop
```

The checked-in map sends only the seven established routes to their owners and reuses upstream connections. A stopped owner yields proxy 502/503 on its routes while live peers remain reachable. A reachable handler with a failed database operation returns JSON 500 `INTERNAL_ERROR`; uncommitted scans and completions are not acknowledged. On database recovery, committed checkout state remains available and Analytics resumes from its durable checkpoint. `/analytics/popular-items` may temporarily show an older complete window while the backlog catches up.

## Scan performance profiling

Set `SCAN_PROFILE=true` on Transaction and Analytics Service to log cumulative aggregate timings for Scan handler, JDBC phases, pool occupancy, and Materializer batches. The default Transaction pool size of 32 was selected from fresh 10- and 100-station A/B benchmarks. See the [Scan optimization evidence](../load-client/reports/scan-optimization/README.md) and `benchmark-scan.ps1` for reproducible commands and limitations.

## SQL Materializer schema upgrade

The current schema gives each accepted Scan an immutable `event_id` and a nullable unique analytics `position`. Transaction Service commits the raw event and basket without locking `scan_sequence`. Analytics Materializer assigns positions in batches and commits the position checkpoint atomically. Existing deployments with the former position-primary-key schema can stop Transaction and Analytics, back up MySQL, run `db/migrate-sql-materializer.sql` once, verify its two counts and checkpoint, then deploy the new JARs. Fresh benchmark resets use the updated `db/init.sql`. Keep MySQL durability settings unchanged.

## Tests and disposable database reset

With all three services, Nginx, and MySQL running:

```bash
./build-tests.sh
SERVER_BASE_URL=http://localhost:8080 ./run-tests.sh
```

`run-tests.sh` uses `DB_USER` and `DB_PASSWORD` from the environment and fails if a live contract/integration test is skipped. The opt-in destructive `ResetBaselineTest` remains disabled by default. JUnit XML is written under `server/out/test-results/`. The live suite exercises independent service restarts, public routing, durable scans, crash recovery, and checkout conservation.

Reset **only the disposable assignment database**. Stop all three JVMs first, since this drops and recreates `cs6510_selfcheckout`, then restart each JVM. On Windows from the repository root:

```powershell
./db/init-db.ps1 -MySqlUser root -MySqlHost 127.0.0.1 -Port 3307
```

The reset prompts for the password and reports exactly 2,000 products, 2,000 inventory rows, and 20,000,000 initial units. With MySQL's CLI on `PATH`, wait for Analytics to catch up after a workload and run:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p cs6510_selfcheckout < db/validate-invariants.sql
```

The script raises SQLSTATE 45000 on a mismatch and prints `PASS` only after stock, transaction lines/totals, contiguous accepted scans, all eligible windows, checkpoint, and independently derived top-ten ranks agree. Run it from the repository root. See [the feature quickstart](../specs/004-distributed-domain-services/quickstart.md) and [load evidence](../load-client/reports/service-based-architecture/README.md) for the two reset-based workloads.
