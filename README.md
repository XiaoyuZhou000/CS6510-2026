# Self-Checkout System — Semester Project

A single API contract, to be implemented by a different
architecture style each week. Each implementation is tested every week by the **same**
unmodified load-testing client. Because the client and the contract never
change, the differences you observe week to week come entirely from the
architecture, not from a different test tool.

## What's in this folder

```
self-checkout-project/
├── spec/
│   └── self-checkout-openapi.yaml   ← the shared API contract (OpenAPI 3.0.3)
├── load-client/
│   ├── src/*.java                   ← the load-testing client (zero dependencies)
│   ├── build.sh
│   └── run.sh
└── mockserver/
    ├── MockServer.java              ← optional reference server (see below)
    ├── build.sh
    └── run.sh
```

## Architecture diagram (event-based, feature 006)

The current deployment is three microservices behind an Nginx gateway, each with its own MySQL schema. The services exchange domain events only through a Kafka broker. The diagram is [docs/architecture/event-based-architecture.md](docs/architecture/event-based-architecture.md), which GitHub renders from Mermaid. An exported copy is [docs/architecture/event-based-architecture.svg](docs/architecture/event-based-architecture.svg). The benchmark evidence is in [load-client/reports/event-based-architecture/](load-client/reports/event-based-architecture/README.md).

## The API contract (`spec/self-checkout-openapi.yaml`)

This is what every weekly implementation must satisfy, regardless of its
internal architecture:

| Endpoint                           | Purpose                                                                   |
| ---------------------------------- | ------------------------------------------------------------------------- |
| `GET /items`                       | Full catalog (SKU, name, price) — the client fetches this once at startup |
| `POST /transactions`               | Start a transaction at a station                                          |
| `POST /transactions/{id}/items`    | Scan one unit of an item into the basket                                  |
| `POST /transactions/{id}/complete` | Pay, decrement stock, return a receipt                                    |
| `GET /transactions/{id}`           | Debugging/instructor use only, not exercised by the client                |
| `GET /inventory/low-stock`         | Current low-stock alerts                                                  |
| `GET /analytics/popular-items`     | Most-scanned items in the current sliding window                          |

Some issues to note:

- **Stock is decremented at  transaction*completion*, not at scan time.** The physical
  metaphor is that the customer already has the item in hand when they scan
  it — the backend's job is just to keep an accurate count, not to gate the
  scan. 
- **Every endpoint is synchronous**, no matter what a given week's internal
  architecture does. Event-driven or orchestration-driven weeks are free to
  use events, queues, or an orchestration engine *internally*, but the
  client-facing contract never changes. This is what keeps the same load
  client valid for every week.
- **Popular items use a hopping window**: the server considers the most
  recent `windowSize` scans (spec default 1000) and recomputes every
  `slideInterval` scans (spec default 500). The response includes
  `windowStart`/`windowEnd` so students can show their work.

## The load client (`load-client/`)

A Java client using the JDK's built-in java.net.http.HttpClient` and a hand-rolled JSON
reader/writer — this removes all external dependencies. The CLI options below default to values to use for each weekly submission. 

### Build & run

Requires a full JDK 21+ (not just a JRE), since javac` needs to be installed:

```bash
cd load-client
./build.sh
./run.sh --baseUrl=http://localhost:8080 --stations=10 --duration=60
```

### CLI options

| Flag                        | Default                 | Meaning                                         |
| --------------------------- | ----------------------- | ----------------------------------------------- |
| `--baseUrl`                 | `http://localhost:8080` | Base URL of the system under test               |
| `--stations`                | `10`                    | Concurrent simulated checkout stations          |
| `--duration`                | `60`                    | Test length in seconds                          |
| `--minItems` / `--maxItems` | `1` / `20`              | Basket size range per transaction               |
| `--popularLimit`            | `10`                    | How many popular items to request at the end    |
| `--requestTimeout`          | `10`                    | Per-request timeout, seconds                    |
| `--verbose`                 | `false`                 | Print every completed transaction as it happens |
| `--reportDir`               | `./reports`             | Where the JSON report file is written           |

Run `./run.sh --help` for the same, from the tool itself.

**Stress mode** is just the same client with a bigger `--stations` value —
e.g. `--stations=200 --duration=180` — useful specifically for the weeks
where scalability differences between styles are the point (microservices,
event-driven), since at the default 10-station scale most architectures
will feel instantly fast regardless of style.

### What the report shows

At the end of a run, the client prints a console report and writes a JSON
file to `--reportDir` (default `./reports`), so results from different
weeks can be diffed or charted later:

- Per-operation (`START_TRANSACTION`, `SCAN_ITEM`, `COMPLETE_TRANSACTION`):
  success/error counts, mean, p50, p95, p99, max latency, and error rate.
  **Use the percentiles, not just the mean** — tail latency is usually
  where an architecture's weaknesses (lock contention, network hops,
  orchestration overhead) actually show up.
- Overall throughput (transactions/sec, items/sec).
- Current low-stock alerts.
- Current most-popular items.

The item-popularity sampling is intentionally **not uniform random** — it
uses a Zipf-like weighting (`ItemSampler.java`) so a small number of items
get scanned disproportionately often, the same way real retail sales work.
Without this, the popular-items feature would have nothing meaningful to
detect.

## The mock server (`mockserver/`)

`MockServer.java` is a bare-bones, single-file reference implementation of
the contract, built the same zero-dependency way as the client (just the
JDK's built-in `com.sun.net.httpserver`). This gives you something real to 

point the client at **before ** any implementation exists. You can see a full 

report end to end and understand the contract by example. 

It is **not** an example of good architecture — it's a handful of
`ConcurrentHashMap`s behind an HTTP server, deliberately uninteresting.


```bash
cd mockserver
./build.sh
./run.sh 8080 2000 10000 50   # port, catalogSize, stockPerItem, lowStockThreshold
```

## The concurrency gotcha

With 10+ stations completing transactions concurrently against the sameinventory, a naive "read stock, check it, then write stock minus one" doneas two separate steps (a read call followed by a write call, or even twonon-atomic statements against a shared database row without appropriatelocking) can let two stations both succeed in buying the last unit of anitem. In the monolith and layered weeks this is easy to get right byaccident, because it's all one process talking to one local transaction. Itgets *much* easier to get wrong once inventory becomes its own service(service-based, microservices) and the check-then-decrement happens acrossa network call.

Suggested correctness check for grading, independent of any performancenumber: **for every SKU, `initial_stock - final_stock` must equal the totalnumber of completed-transaction line items for that SKU, and final stockmust never go negative.** A student's implementation can pass everyfunctional test and still fail this invariant under load — that's thepoint.



## Grading - things to look for

Submit the timestamped JSON report for each architecture in a 
`reports/report-*.json` directory alongside your code, and compare:

- Does the system complete runs under normal and stress mode workloads. How do latencies compare?
- How do p95/p99 latencies move relative to the previous week's numbers on
  the same hardware? 
- Does the popular-items ranking stay stable across implementations (it
  should — it's testing the analytics feature, not the architecture)?


