# Quickstart: Validate Pipeline Analytics

This guide validates the implemented three-stage pipeline without changing the OpenAPI file,
database baseline, or load client.

## Prerequisites

- JDK 21+ (`java` and `javac`)
- Bash (Git Bash/MSYS, WSL, Linux, or macOS)
- MySQL 8.0+ and its command-line client
- Disposable access to `cs6510_selfcheckout` (defaults: `127.0.0.1:3307`, user `root`)

HTTP/database tests may self-skip when live services are unavailable. A skip is useful during local
unit work but is not final validation evidence.

## 1. Build and run automated gates

From the repository root:

```bash
cd server
./build.sh
./build-tests.sh
./run-tests.sh
```

Expected: all unit and architecture tests pass. The implemented suite must prove:

- three independently executing filters connected through FIFO `BlockingQueue` pipes;
- immutable messages, exact 999/1,000/1,500 boundaries, and chronological 500-event overlap;
- deterministic ties, contiguous top-10 ranks, and store-only query visibility;
- concurrent no-loss ingestion, restart warm-up, retry without overtaking, and atomic writes;
- clean drain, boundary-vs-shutdown behavior, forced timeout, worker failure, and JSON log shape.

On Windows, run these scripts from Git Bash. If `bash` in PowerShell resolves to WSL but no WSL
distribution is installed, invoke Git Bash explicitly (for example,
`& 'C:\Program Files\Git\bin\bash.exe' -lc 'export PATH="/usr/bin:/mingw64/bin:$PATH"; ./server/build.sh && ./server/build-tests.sh && ./server/run-tests.sh'`).
This changes only the shell used to execute the same repository scripts and does not change the
test selection or the zero-failure requirement.

## 2. Reset the canonical baseline

Stop any running server. From PowerShell at the repository root:

```powershell
./db/init-db.ps1 -MySqlUser root -MySqlHost 127.0.0.1 -Port 3307
```

Expected: 2,000 catalog items, 2,000 inventory rows, and 20,000,000 total units. Always start a
fresh server after reset because catalog, baskets, positions, rings, queues, and workers are loaded
or created once per process.

## 3. Start the server

From Bash at the repository root:

```bash
./server/build.sh
./server/run.sh 8080 127.0.0.1 3307 cs6510_selfcheckout root
```

Prefer `DB_PASSWORD` over placing a password in shell history. Optionally set
`ANALYTICS_SHUTDOWN_TIMEOUT_MS`; omission uses 5,000 ms. Expected: all three analytics workers
start and the HTTP server listens on port 8080.

## 4. Run the live contract and integration gate

In another Bash shell:

```bash
cd server
./run-tests.sh \
  -DSERVER_BASE_URL=http://localhost:8080 \
  -DDB_URL='jdbc:mysql://127.0.0.1:3307/cs6510_selfcheckout?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC' \
  -DDB_USER=root \
  -DDB_PASS="$DB_PASSWORD"
```

Expected: zero failed tests and zero skipped live checks. The public response remains compatible
with [public-api-compatibility.md](contracts/public-api-compatibility.md); controlled windows start
at `[1,1000]` and `[501,1500]`; concurrent input has exact membership; a failed rank insert exposes
no partial window.

## 5. Run and validate the default workload

With the clean server running:

```bash
cd load-client
./build.sh
./run.sh --baseUrl=http://localhost:8080 --stations=10 --duration=60 --reportDir=./reports
```

Expected: one new timestamped JSON report records 10 stations and 60 seconds. Target p95 latency is
start below 200 ms, scan below 100 ms, and completion below 1 second. The popular result, if
queried, describes only a complete committed window.

From the repository root, validate durable invariants:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p cs6510_selfcheckout < db/validate-invariants.sql
```

Expected: all checks report `PASS`, including non-negative inventory, reconciliation, atomic
completion, and coherent popular-window ranks.

## 6. Reset and run the stress workload

Stop the server. Repeat steps 2 and 3 so both MySQL and in-process state are fresh, then run:

```bash
cd load-client
./run.sh --baseUrl=http://localhost:8080 --stations=100 --duration=120 --reportDir=./reports
```

Run `db/validate-invariants.sql` again. Expected: a distinct timestamped JSON report records 100
stations and 120 seconds; checkout/inventory invariants hold; no accepted analytics event is
silently lost; structured backlog records identify any queue growth.

## 7. Exercise lifecycle failure scenarios

The automated suite must use fakes and short injected deadlines to verify these deterministic
cases; do not create ad-hoc production endpoints:

1. A failing store retains `[1,1000]`; `[501,1500]` cannot become visible first; after recovery both
   commit in order.
2. Restart from persisted end 1,000 publishes nothing after 500 fresh scans and publishes
   `[1001,2000]` after 1,000 fresh scans.
3. Shutdown at an event boundary either admits the event before the drain marker and processes it,
   or rejects it with a structured record; it never queues the event after the marker.
4. A permanent store failure exceeds the injected/default deadline, terminates forcibly, and emits
   `SHUTDOWN_FORCED` but not `SHUTDOWN_COMPLETED`.
5. An unexpected worker failure emits `WORKER_FAILED`, names the stage, rejects later ingestion,
   and leaves the latest committed query available.

See [pipeline-messages.md](contracts/pipeline-messages.md) and
[lifecycle-observability.md](contracts/lifecycle-observability.md) for the normative assertions.

## 8. Retain submission evidence

Select exactly one new default report and one new stress report for this iteration, each created
after its own reset and server restart. Historical files already exist in `load-client/reports/`,
so place or reference the two selected artifacts from a dedicated iteration evidence location
rather than claiming the historical directory contains only two files. Record invariant `PASS`
output and relevant structured log excerpts without changing the client-generated report content.
