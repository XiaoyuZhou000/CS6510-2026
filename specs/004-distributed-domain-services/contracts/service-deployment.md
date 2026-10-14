# Service Deployment Contract

The planned deployment has three business JVMs and one static HTTP router. The router provides the single public URL required by the unchanged client; it owns no business data or rules. See [public-http-and-routing.md](public-http-and-routing.md) for route mapping.

| Unit | Build output | Entry point | Default HTTP port | Dependencies |
|---|---|---|---:|---|
| Transaction | `server/out/transaction.jar` | `transaction.TransactionMain` | 8081 | common jar, Connector/J, central MySQL |
| Inventory | `server/out/inventory.jar` | `inventory.InventoryMain` | 8082 | common jar, Connector/J, central MySQL |
| Analytics | `server/out/analytics.jar` | `analytics.AnalyticsMain` | 8083 | common jar, Connector/J, central MySQL |
| Static router | `deploy/nginx.conf` | Nginx | 8080 | Three upstream ports; no MySQL access |

Each Java artifact is built and launched separately with its own process, HTTP executor, database pool, configuration, and shutdown hook. Common is a build-time utility dependency only. Database host/port/name/user/password and pool size use documented environment variables (retaining the existing `DB_*` names); each service has its own `*_PORT` variable and optional command-line port override. Credentials are supplied at runtime, never committed. Startup fails clearly if required database connections cannot be established; reconnection handling must not turn failed writes into success. Analytics worker shutdown and replay are local to Analytics.

The build/run scripts planned under `server/` support separate `build.sh transaction|inventory|analytics` and `run-transaction.sh`, `run-inventory.sh`, `run-analytics.sh` commands. Tests may build all outputs. The router config is launched separately and stays running during a single service restart. Stopping Transaction affects transaction routes only; stopping Inventory affects catalog/low-stock routes only; stopping Analytics affects popular-items only. A shared database outage affects database-dependent routes across services.

The `db/init.sql` reset remains the single destructive baseline reset. Stop all three services before resetting it, then restart them so no old pool, cache, or checkpoint state survives. The baseline remains 2,000 products and 10,000 units per product. The unchanged `load-client/` Java entry point and JSON report format remain the load contract.
