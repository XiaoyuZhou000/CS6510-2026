# Phase 0 Research: Layered Architecture Migration

All technical-context unknowns are resolved.

## 1. Migration shape

**Decision**: Keep the server as one Java 21 process and implement layers as enforceable package
and interface boundaries.

**Rationale**: The assignment asks for responsibility separation, while the fixed client expects
one synchronous service. An in-process refactor isolates change without adding network latency,
distributed failure modes, or contract changes.

**Alternatives considered**: Separate transaction, inventory, and analytics services were rejected
because that would be a microservice migration rather than the requested layered migration.

## 2. Layer ownership

**Decision**: Assign HTTP/bootstrap/JSON to API; checkout, catalog, low-stock policy, pricing, and
open baskets to transaction; scan-window recording/ranking/query behavior to analytics; and all
SQL, JDBC connections, transactions, and persistence mapping to database access.

**Rationale**: This assigns every component to exactly one of the four required layers. Catalog and
low-stock behavior fit the store transaction domain and cannot remain in handlers without creating
API business logic or direct persistence bypasses.

**Alternatives considered**: A fifth shared/domain layer was rejected because the specification
requires exhaustive assignment to four layers. Leaving feature-named packages in place was
rejected because the current runtime still crosses the intended boundaries.

## 3. Explicit contracts and dependency direction

**Decision**: API handlers depend on `TransactionOperations`, `CatalogOperations`,
`InventoryOperations`, or `AnalyticsOperations`. Transaction and analytics depend on narrowly
scoped database contracts. Each contract owns its immutable input/output records; DAO nested types
do not cross layers. Architecture tests inspect all source packages/imports and fail on unassigned
files, forbidden imports, cycles, direct SQL/JDBC outside `database`, and handler-to-store calls.

**Rationale**: The current code has business-to-API coupling, handler-to-DAO bypasses, and an
analytics↔persistence cycle. Contract-owned records make collaborators replaceable without a
catch-all shared package.

**Alternatives considered**: A global `model` package was rejected as an unowned fifth layer.
Depending on concrete DAOs was rejected because it prevents isolation and permits bypasses.

## 4. Atomic checkout completion

**Decision**: Replace the transaction service's raw `Connection` orchestration with one coarse
`CheckoutCompletionStore.completeAtomically` operation. Its JDBC implementation performs, in one
MySQL transaction: guarded `OPEN → COMPLETED`, completed-line inserts, ascending-SKU conditional
decrements, and commit. It returns a typed result (`COMPLETED`, `NOT_OPEN`, or
`INSUFFICIENT_STOCK`) only after commit/rollback.

**Rationale**: Database transaction demarcation belongs to the database-access layer, while the
transaction layer remains responsible for deciding the business response. A coarse operation makes
the all-or-nothing invariant impossible to bypass accidentally and keeps `java.sql.Connection`
out of business code.

**Alternatives considered**: Fine-grained repositories plus a generic unit of work were rejected
because they leak persistence sequencing upward. Separate auto-commit calls were rejected because
they can partially complete a sale. Serializable isolation was rejected because InnoDB row locks
and conditional updates already provide the needed guarantee at lower contention cost.

## 5. Completion, basket, and pricing semantics

**Decision**: Preserve the concurrent in-memory basket map, per-basket synchronization, scan-time
price capture, durable transaction row at start, and eviction after successful completion.
Duplicate completion remains guarded by the persisted transaction status, and receipts are exposed
only after the database gateway reports a committed result.

**Rationale**: These behaviors already satisfy consistency, idempotency, durability, and scan
latency requirements. The migration should change ownership and coupling, not benchmark semantics.

**Alternatives considered**: Persisting every scan or reconstructing open baskets after restart was
rejected because it changes write load and the accepted recovery/performance trade-off.

## 6. Accepted-scan analytics coordination

**Decision**: The transaction service receives an explicit `AcceptedScanSink`. `api.Main` wires
that sink to `AnalyticsOperations.recordAcceptedScan` with a method reference. The transaction
service invokes it only after basket admission succeeds. Neither business package imports the
other; the composition root is the only component aware of both contracts.

**Rationale**: Popularity counts accepted scans rather than completed purchases. This preserves
ordering in the scan use case without putting analytics policy in HTTP code or creating a static
transaction↔analytics package dependency.

**Alternatives considered**: Having the HTTP handler call two services was rejected because it
would own business sequencing. A transaction import of an analytics concrete class and an
analytics implementation of a transaction-owned interface were rejected because both create a
lateral package dependency.

## 7. Analytics window behavior

**Decision**: Preserve the synchronized global counter/ring buffer, exact 1,000-scan snapshots
every 500 accepted scans, single ordered checkpoint executor, retry-in-place behavior, startup
counter recovery, first-invalid-boundary skip after restart, and latest committed-window reads.
The analytics layer owns ranking and deterministic tie handling; the database layer only maps and
persists supplied results.

**Rationale**: Scans stay independent of database latency while computed windows remain coherent
and ordered. Moving query behavior out of `AnalyticsHandlers` removes the existing API-to-DAO
bypass.

**Alternatives considered**: Synchronous checkpoint writes were rejected because they couple scan
latency to MySQL. Per-scan persistence was rejected because it adds a write to the hottest path.
Parallel checkpoint writers were rejected because later windows could overtake earlier failures.

## 8. Schema strategy

**Decision**: Reuse `db/init.sql` and its six tables without a migration. Database adapters map the
existing schema into layer-contract records.

**Rationale**: Layer boundaries do not require a storage change, and preserving the schema keeps
reset behavior and before/after comparisons stable.

**Alternatives considered**: Adding a new domain schema or durable scan log was rejected as scope
expansion. A unique popular-window key can be considered later as resilience hardening but is not
required to demonstrate this architectural migration.

## 9. API compatibility and error mapping

**Decision**: Keep all seven routes, methods, status codes, JSON shapes, and synchronous behavior
from `spec/self-checkout-openapi.yaml`. Domain/database failures carry transport-neutral codes;
`ApiErrorMapper` translates each once into the contract's `{error,message}` response. Completion
continues to tolerate the load client's empty JSON body.

**Rationale**: The client checks exact statuses and relies on the catalog at startup. Removing
HTTP status fields and API constants from business code fixes the current reverse dependency while
preserving behavior.

**Alternatives considered**: Changing the OpenAPI or client was rejected by the contract gate.
Allowing each handler to invent mappings was rejected because mappings could diverge.

## 10. Testing and validation

**Decision**: Retain the current 39 behavior tests, then add: API tests with fake business
contracts; transaction tests with fake stores and scan sink; analytics tests with a fake window
store; MySQL adapter tests; and an exhaustive architecture test. Live HTTP/DB validation must not
be reported as complete when tests self-skip. After implementation, reset and restart separately
for 10×60 and 100×120 workloads, retain both reports, and reconcile stock to completed lines.

**Rationale**: Existing tests characterize behavior but do not prove the new boundaries. Isolation
tests and a static dependency gate prove that the migration is structural rather than cosmetic.

**Alternatives considered**: Relying only on end-to-end tests was rejected because correct HTTP
responses cannot detect direct DAO bypasses, cycles, or misplaced policy.

## 11. Toolchain

**Decision**: Retain plain `javac`/Bash scripts, the vendored JUnit Platform Console 1.11.0 and
MySQL Connector/J 9.0.0, JDK virtual threads, and the existing PowerShell database reset script.

**Rationale**: No framework is needed to implement testable layering, and keeping the toolchain
fixed makes load results comparable.

**Alternatives considered**: Maven/Gradle and dependency-injection frameworks were rejected as
unnecessary changes for this iteration.
