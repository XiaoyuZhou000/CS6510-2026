# Distributed Service Load Evidence — 2026-10-13

These are two distinct, byte-for-byte copies of JSON written by the **unchanged** `load-client/src/` client against the actual Nginx `localhost:8080` proxy and three Java 21 service JVMs. They are not copied from an earlier architecture. Nginx 1.31.6 routed to Transaction `:8081`, Inventory `:8082`, and Analytics `:8083`; all used the same MySQL 8.4.4 database on `127.0.0.1:3307`. The checked-in `deploy/nginx.conf` enabled upstream connection reuse after a diagnostic run exposed Windows temporary TCP-port exhaustion. No client source or OpenAPI changes were made.

## Reproduction and provenance

1. Build with `./server/build.sh` and `./load-client/build.sh`; start all three service JARs and Nginx as documented in [server/README.md](../../../server/README.md). Confirm `GET http://localhost:8080/items` returns 2,000 items.
2. Stop all three service processes. Run `./db/init-db.ps1 -MySqlUser root -MySqlHost 127.0.0.1 -Port 3307`; it printed 2,000 catalog rows, 2,000 inventory rows, and 20,000,000 initial units. Restart all three services and the proxy. From `load-client/`, run **exactly** `java -cp out Main`. It wrote `reports/report-20261013-153843.json`; copy it byte for byte to `default.json`.
3. After Analytics catches up, run `db/validate-invariants.sql`. Its actual output is [default-invariants.txt](default-invariants.txt).
4. Stop all three services and the proxy, perform a **separate** `db/init-db.ps1` reset with the same printed baseline, and restart all four processes. From `load-client/`, run **exactly** `java -cp out Main --stations=100 --duration=120`. It wrote `reports/report-20261013-154141.json`; copy it byte for byte to `stress.json`.
5. Wait for Analytics catch-up and run the same invariant script. Its actual output is [stress-invariants.txt](stress-invariants.txt). The script checks every eligible window and top-ten rank against durable accepted scans, plus transaction totals, event order, and stock reconciliation.

The JARs used for both runs had these SHA-256 hashes (generated build artifacts, not committed):

| Artifact | SHA-256 |
|---|---|
| `transaction.jar` | `07D542B6AFA0EB54E9742A942F15455B5D0E72BA723D82CC4B68306BBCD3160D` |
| `inventory.jar` | `B7BF8106DA5E0FC8E30B6CD66BA25EA708F7BB27D93C3404348BF7D18E1C41CF` |
| `analytics.jar` | `B9E58E7D8266D867383E3A8EBBA31AD82F1FD9CD91E65E02726C9125B99887A3` |
| `common.jar` | `A05B85A5A20C7D69B9997905CF061EC6F52753864B437DDE7E39C292B68D3BF8` |

| Report | Original client timestamp (UTC) | SHA-256 of retained JSON | Settings | Throughput |
|---|---|---|---|---|
| [default.json](default.json) | 2026-10-13 22:38:43 | `A5D1E3B75919FAD0FE3137B4667AFBE472E28CE62DA31A1B6FA8A37F8BDEF393` | 10 stations, 60 seconds | 16.55 completed transactions/s; 172.76 acknowledged items/s |
| [stress.json](stress.json) | 2026-10-13 22:41:41 | `57DC401F97CC8F02C765D47BBD49D0062395DB6646A5CBECABD38881DB5FE1C5` | 100 stations, 120 seconds | 17.14 completed transactions/s; 177.23 acknowledged items/s |

## Measured operation outcomes

| Run | Operation | Success | Errors | Error rate | p95 / p99 latency (ms) |
|---|---|---:|---:|---:|---:|
| Default | Start transaction | 999 | 0 | 0% | 10.34 / 181.79 |
| Default | Scan item | 10,426 | 0 | 0% | 74.10 / 140.07 |
| Default | Complete transaction | 999 | 0 | 0% | 14.43 / 26.58 |
| Stress | Start transaction | 2,121 | 6 | 0.2821% | 1,824.56 / 4,660.31 |
| Stress | Scan item | 21,926 | 1 | 0.0046% | 1,446.49 / 2,836.81 |
| Stress | Complete transaction | 2,120 | 0 | 0% | 1,610.90 / 3,419.12 |

All seven stress errors were client-side 10-second request timeouts. The database has 21,927 committed accepted scans, one more than the client's acknowledged scan count. The timed-out scan committed, as permitted by the existing lost-response contract. There are 2,120 completed and seven OPEN transactions after stress; the six timed-out starts and one interrupted basket account for that durable state. Client-side timeout does not imply a rolled-back commit. Do not retry a scan blindly as if it had an idempotency key.

## Database and recovery verification

| Run | Invariants | Completed transactions | Durable accepted scans | Minimum stock | Complete windows | Latest window end |
|---|---|---:|---:|---:|---:|---:|
| Default | PASS | 999 | 10,426 | 8,714 | 19 | 10,000 |
| Stress | PASS | 2,120 | 21,927 | 7,362 | 42 | 21,500 |

The final Phase 8 live JUnit run through the proxy reported 137 successful tests, zero failures, no skipped live contract/integration tests, and one intentionally disabled destructive reset test. It included the independent three-JAR restart smoke test, owner outage routing checks, acknowledged OPEN basket restart, Analytics crash points, downtime replay, concurrent completion, and public contract tests. The SQL files above establish stock reconciliation and the exact rank/count result of every eligible durable-scan window after Analytics caught up.

## Phase 8 final verification

The following commands ran on Windows from the repository root unless a command first changed into server/. The installed Nginx binary was the existing 1.31.6 executable at the path shown. DB_PASSWORD and DB_USER were already set in the environment; the password was not passed as a command argument.

    cd server
    & 'C:\Program Files\Git\bin\bash.exe' -c 'export PATH=/usr/bin:/mingw64/bin:$PATH; ./build-tests.sh'
    $env:SERVER_BASE_URL='http://localhost:8080'
    $env:NGINX_BIN='C:\Users\18422\AppData\Local\Temp\cs6510-nginx\nginx-1.31.6\nginx.exe'
    & 'C:\Program Files\Git\bin\bash.exe' -c 'export PATH=/usr/bin:/mingw64/bin:$PATH; ./run-tests.sh' *> out/phase8-live-tests.txt
    cd ..
    $env:MYSQL_PWD=$env:DB_PASSWORD
    Get-Content db/validate-invariants.sql | & 'C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe' -h 127.0.0.1 -P 3307 -u $env:DB_USER cs6510_selfcheckout *> server/out/phase8-invariants.txt
    Remove-Item Env:MYSQL_PWD

The build compiled common.jar, transaction.jar, inventory.jar, analytics.jar, and all tests without the former out/main classpath. The strict test run exited 0: 138 tests found, 137 successful, zero failed. The only aborted test was the opt-in destructive ResetBaselineTest; the JUnit XML in server/out/test-results/run-20261013163150-12 has no skipped live contract or integration case. Analytics had reached window end 22,000 for 22,152 accepted positions before SQL validation. The final invariant command exited 0 and printed PASS: 2,204 completed transactions, 22,152 accepted units and positions, minimum stock 7,350, 43 complete windows, and checkpoint 22,000.

An earlier diagnostic invariant run found two stock mismatches caused by the live ConcurrentCompletionTest and LowStockEndpointTest restoring fixture stock without accounting for their completed sales. Their cleanup was corrected, the two affected rows in the disposable assignment database were reconciled, and both the strict suite and full invariant SQL were rerun successfully. The saved default.json, stress.json, and their original invariant outputs were not modified.

## Client report schema and prior pipeline comparison

Both retained JSON reports match the unchanged load-client/src/ReportWriter.java output structure. Each passed 30 structural and arithmetic checks: exact root/config/operation fields, all three operation names in order, nonnegative counts and ordered latency percentiles, totals and rates consistent with wall-clock duration, and the ten popular-item fields/ranks. Both contain an empty lowStockAlerts array. The immutable load-client/src/ source set and spec/self-checkout-openapi.yaml have no Git diff; PipelineArchitectureTest also checks their established SHA-256 values.

| Workload | Architecture | Completed transactions/s | Acknowledged items/s | Scan p95 / p99 (ms) |
|---|---|---:|---:|---:|
| Default, 10 stations / 60 s | Prior pipeline | 153.47 | 2,144.35 | 0.2407 / 0.4255 |
| Default, 10 stations / 60 s | Distributed durable services | 16.55 | 172.76 | 74.0993 / 140.0731 |
| Stress, 100 stations / 120 s | Prior pipeline | 159.42 | 3,698.38 | 0.2559 / 0.4600 |
| Stress, 100 stations / 120 s | Distributed durable services | 17.14 | 177.23 | 1,446.4857 / 2,836.8112 |

The distributed runs show a material throughput and scan tail-latency regression. Each acknowledged scan now commits its basket state and event through MySQL, including allocation under the serialized scan-sequence row; the old pipeline acknowledged scans in process memory. These reports measure the combined deployment and durability cost and do not isolate the sequence lock's individual contribution. The stress run also recorded one scan timeout while the database retained its committed event. No numerical performance target was assumed. A targeted optimization decision belongs in the planned speckit-converge review after this evidence is inspected.