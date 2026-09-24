# Layer Ownership and Dependency Rules

## Permitted runtime collaboration

```text
HTTP/client
    │
    ▼
api.Main
    ├──► TransactionHttpHandler ──► TransactionOperations ◄── TransactionService
    ├──► CatalogHttpHandler ──────► CatalogOperations ───────┐
    ├──► InventoryHttpHandler ────► InventoryOperations ─────┴── StoreQueryService
    └──► AnalyticsHttpHandler ────► AnalyticsOperations ◄─────── AnalyticsService
                                      ▲                              │
TransactionService ──► AcceptedScanSink┘                              │
    │                                                                 │
    ├──► TransactionStore ─────────► JdbcTransactionStore             │
    ├──► CheckoutCompletionStore ──► JdbcCheckoutCompletionStore      │
    └──► CatalogCache ─────────────► CatalogStore ◄── JdbcCatalogStore│
                                                                      │
StoreQueryService ────────────────► InventoryStore ◄── JdbcInventoryStore
AnalyticsService ─────────────────► PopularWindowStore ◄── JdbcPopularWindowStore

All Jdbc* adapters ──► ConnectionPool ──► MySQL
```

The accepted-scan callback is constructed in `api.Main` from the two public business contracts;
transaction and analytics packages do not import each other. `api.Main` may instantiate concrete
database adapters solely during startup. No API handler or post-start request path may retain or
call a database contract.

## Import rules

| Package | May import project packages | Forbidden examples |
|---|---|---|
| `api` handlers/utilities | `transaction`, `analytics`, `api` | `database`, JDBC/SQL, concrete stores |
| `api.Main` | all four layers for construction only | SQL statements, request-time store calls, business rules |
| `transaction` | `database` contracts, `transaction` | `api`, `analytics`, JDBC/SQL, `Jdbc*` implementations |
| `analytics` | `database` contracts, `analytics` | `api`, `transaction`, JDBC/SQL, `Jdbc*` implementations |
| `database` | `database`, JDK/JDBC | `api`, `transaction`, `analytics` service or DTO types |

Database contract input/output records therefore belong to `database`; transaction/analytics map
them to their own public records. This keeps the database implementation from depending upward.

## Final component ownership

| Layer | Implemented components |
|---|---|
| API | `Main`, `TransactionHttpHandler`, `CatalogHttpHandler`, `InventoryHttpHandler`, `AnalyticsHttpHandler`, `ApiErrorMapper`, `ApiErrors`, `HttpSupport`, `api.json.Json` |
| Transaction | `TransactionOperations`, `TransactionService`, `Basket`, `AcceptedScanSink`, `CatalogOperations`, `InventoryOperations`, `StoreQueryService`, `CatalogCache`, `LowStockThresholds`, `TransactionFailure` |
| Analytics | `AnalyticsOperations`, `AnalyticsService`, `WindowMath` |
| Database access | `CatalogStore`, `TransactionStore`, `InventoryStore`, `CheckoutCompletionStore`, `PopularWindowStore`, their five `Jdbc*` adapters, `ConnectionPool`, `StoreFailure` |

## Automated architecture gate

The architecture test must:

1. enumerate every production `.java` file and assign it to exactly one of the four packages;
2. reject forbidden package imports and package cycles;
3. reject `java.sql`, SQL statement text, `ConnectionPool`, and `Jdbc*` references outside
   `database` (except class construction in `api.Main`);
4. reject database fields/constructor parameters in HTTP handler classes;
5. reject HTTP types/status constants in transaction, analytics, and database packages;
6. verify `api.Main` performs construction/registration only and handlers receive business
   interfaces rather than stores;
7. fail when a new production component has no declared layer.

Passing endpoint tests alone does not satisfy this gate.
