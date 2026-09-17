# Architectural Characteristics Analysis — Supermarket Self-Checkout System

## 1. Required Architectural Characteristics

The monolithic server must follow the provided OpenAPI contract and support concurrent checkout stations. The characteristics below describe my design goals; the latency thresholds are proposed targets, not measured results or thresholds specified by the assignment.

| Characteristic | Concrete requirements |
| --- | --- |
| **Consistency** | Decrease stock only when a transaction completes, never when an item is scanned. For each SKU, `initial_stock - final_stock` must equal the total quantity sold in completed transactions, and stock must never become negative. Concurrent completions must not sell the same last unit, and completing the same transaction twice must not decrement stock twice. |
| **Reliability and durability** | Persist inventory and popular-item results in a database, as required by the assignment. My design also calls for saving completed transactions before returning a successful receipt. A failure at one checkout station must not corrupt another station's basket or the shared inventory. |
| **Performance** | Under the default 10-station workload, target p95 response times below 200 ms for starting a transaction, 100 ms for scanning an item, and 1 second for completing a transaction. A p95 target means at least 95% of requests should finish within that time. |
| **Scalability** | Support both the default run of 10 concurrent stations for 60 seconds and the required stress run of 100 stations for 120 seconds. Both runs must preserve inventory correctness. Compare throughput, error rates, and p95/p99 latency to identify how the monolith handles increased load. |
| **Analytics accuracy** | Count scans using a hopping window: consider the most recent 1,000 scans and recompute every 500 scans. Persist the top 10 items for the latest computed window and return them through `/analytics/popular-items`, with the corresponding window boundaries. Counts must be accurate for that window, even though the ranking is not refreshed after every scan. |
| **Testability and repeatability** | Make the database easy to reinitialize before each run with 2,000 catalog items and 10,000 units per item. Use the unchanged load client and save the JSON reports so normal and stress results can be compared from the same starting conditions. |

## 2. Top Three Priorities and Their Trade-offs

I prioritize **consistency, reliability, and performance**, in that order. The following choices describe the intended design rather than verified implementation behavior.

### 1. Consistency

Correct inventory is the highest priority. A fast checkout is not useful if concurrent purchases produce incorrect stock counts.

At completion, I would use a database transaction with appropriate locking or conditional updates to check stock, decrement all items in the basket, and save the completed transaction together. If the basket cannot be fulfilled, the operation must not leave partial stock deductions. The transaction's status must also be checked safely so that duplicate completion requests cannot deduct stock again.

**Trade-off:** Coordinating updates adds work and can make requests wait, especially when many customers buy the same popular item. This can increase completion latency and limit throughput under stress. I accept those costs to preserve correct inventory.

### 2. Reliability

A successful completion should mean that the inventory changes and completed transaction have been committed to the database. Required popular-item results must also be persisted. In-progress baskets can remain in the server's memory to reduce database writes during scanning.

**Trade-off:** Waiting for a database commit makes completion slower than acknowledging an in-memory update. Keeping unfinished baskets in memory also limits recovery: a server crash can lose those baskets, requiring customers to restart. A checkout-station crash alone does not necessarily erase a basket held by the server. This design prioritizes durable completed transactions while accepting weaker recovery for unfinished work.

### 3. Performance

Starting a transaction and scanning items should respond quickly because customers repeat these actions throughout checkout. Keeping in-progress baskets in memory reduces database access. Popular-item rankings are recomputed at the required 500-scan intervals.

**Trade-off:** In-memory baskets improve response time but can be lost if the server fails. 