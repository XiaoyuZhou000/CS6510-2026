# Layered Architecture Data Flow

This document describes the target runtime data flow for the migration from the existing
monolithic server to the four-layer design required by the project constitution.

The diagrams show logical responsibilities and boundary crossings. They do not prescribe
specific classes or frameworks.

## Architecture Overview

```mermaid
flowchart TD
    Client[Checkout station or load client]

    subgraph API[API layer]
        Handler[Validate requests and translate responses]
    end

    subgraph Transaction[Transaction layer]
        Checkout[Checkout operations]
        Basket[(In-memory baskets)]
    end

    subgraph Analytics[Analytics layer]
        Recorder[Scan recorder]
        Window[(1000-scan window)]
        Ranking[Top-10 calculation]
    end

    subgraph Data[Database-access layer]
        CatalogAccess[Catalog access]
        InventoryAccess[Inventory access]
        TransactionAccess[Transaction access]
        AnalyticsAccess[Popular-window access]
    end

    DB[(Database)]

    Client --> Handler

    Handler -->|Checkout command| Checkout
    Checkout <--> Basket
    Checkout --> CatalogAccess
    Checkout --> InventoryAccess
    Checkout --> TransactionAccess

    Checkout -->|Result with accepted scan event| Handler
    Handler -->|Forward accepted event| Recorder
    Recorder --> Window
    Window -->|Checkpoint boundary| Ranking
    Ranking --> AnalyticsAccess

    Handler -->|Popular-items query| Recorder
    Recorder --> AnalyticsAccess

    CatalogAccess --> DB
    InventoryAccess --> DB
    TransactionAccess --> DB
    AnalyticsAccess --> DB

    Handler --> Client
```

The API layer performs transport-related work only. The transaction and analytics layers own
business behavior, while the database-access layer is the sole boundary for durable reads and
writes.

## Start Transaction

```mermaid
sequenceDiagram
    participant Client as Checkout Client
    participant API as API Layer
    participant Transaction as Transaction Layer
    participant Basket as In-Memory Basket Store
    participant Data as Database Access
    participant DB as Database

    Client->>API: Request new transaction
    API->>API: Validate station identifier
    API->>Transaction: Start transaction
    Transaction->>Transaction: Generate ID and start time
    Transaction->>Data: Create open transaction
    Data->>DB: Insert open transaction

    alt Database write succeeds
        DB-->>Data: Commit confirmed
        Data-->>Transaction: Transaction persisted
        Transaction->>Basket: Store empty open basket
        Basket-->>Transaction: Basket stored
        Transaction-->>API: New transaction
        API-->>Client: Created response
    else Database write fails
        DB-->>Data: Write error
        Data-->>Transaction: Persistence failure
        Transaction-->>API: Start failed
        API-->>Client: Error response
    end
```

The server returns a transaction identifier only after the open transaction has been accepted.
No basket is created when persistence fails.

## Scan Item

```mermaid
sequenceDiagram
    participant Client as Checkout Client
    participant API as API Layer
    participant Transaction as Transaction Layer
    participant Basket as In-Memory Basket Store
    participant Data as Database Access
    participant DB as Database
    participant Analytics as Analytics Layer
    participant Window as Scan Window

    Client->>API: Submit item scan
    API->>API: Validate request format
    API->>Transaction: Scan item
    Transaction->>Basket: Find basket
    Basket-->>Transaction: Basket state

    alt Basket is missing
        Transaction->>Data: Look up transaction state
        Data->>DB: Read transaction
        DB-->>Data: State or not found
        Data-->>Transaction: Lookup result
        Transaction-->>API: Not found or not open
        API-->>Client: Error response
    else Basket exists
        Transaction->>Data: Find catalog item
        Data->>DB: Read item information
        DB-->>Data: Item or not found
        Data-->>Transaction: Catalog result

        alt SKU is unknown
            Transaction-->>API: SKU not found
            API-->>Client: Not-found response
        else Basket is not open
            Transaction-->>API: Transaction not open
            API-->>Client: Conflict response
        else Scan is accepted
            Transaction->>Basket: Add one unit atomically
            Basket-->>Transaction: Updated count and total
            Transaction-->>API: Scan result and accepted event
            API->>Analytics: Forward accepted scan
            Analytics->>Window: Record SKU
            Window-->>Analytics: Scan recorded
            Analytics-->>API: Accepted
            API-->>Client: Successful scan response
        end
    end
```

Only accepted scans reach analytics. Scanning changes the in-progress basket but does not
decrement durable inventory.

## Complete Transaction

```mermaid
sequenceDiagram
    participant Client as Checkout Client
    participant API as API Layer
    participant Transaction as Transaction Layer
    participant Basket as In-Memory Basket Store
    participant Data as Database Access
    participant DB as Database

    Client->>API: Request transaction completion
    API->>Transaction: Complete transaction
    Transaction->>Basket: Reserve completion snapshot
    Basket-->>Transaction: Items, quantities, and total
    Transaction->>Data: Commit completed sale
    Data->>DB: Begin database transaction
    Data->>DB: Check completion state
    Data->>DB: Verify available stock
    Data->>DB: Store completed transaction and lines
    Data->>DB: Decrement inventory

    alt Every operation succeeds
        Data->>DB: Commit
        DB-->>Data: Durable result
        Data-->>Transaction: Completed sale
        Transaction->>Basket: Mark completed and remove basket
        Transaction-->>API: Receipt
        API-->>Client: Successful receipt response
    else Any operation fails
        Data->>DB: Roll back
        DB-->>Data: No changes committed
        Data-->>Transaction: Completion failure
        Transaction->>Basket: Reopen completion attempt
        Transaction-->>API: Completion error
        API-->>Client: Error response
    end
```

The stock checks, stock decrements, transaction record, transaction lines, and idempotency
decision form one atomic durable operation. A successful response is returned only after commit.

## Analytics Ingestion and Recalculation

```mermaid
sequenceDiagram
    participant API as API Layer
    participant Analytics as Analytics Layer
    participant Window as Scan Window
    participant Worker as Analytics Worker
    participant Data as Database Access
    participant DB as Database

    API->>Analytics: Record accepted scan
    Analytics->>Window: Add scanned SKU
    Window->>Window: Increment scan counter

    alt Fewer than 1000 scans recorded
        Window-->>Analytics: No complete window
    else Not a checkpoint boundary
        Window-->>Analytics: Window updated
    else Checkpoint boundary reached
        Window->>Window: Copy latest 1000 scans
        Window-->>Analytics: Snapshot and boundaries
        Analytics->>Worker: Queue checkpoint
    end

    Analytics-->>API: Scan recorded

    opt Checkpoint was queued
        Worker->>Worker: Count scans by SKU
        Worker->>Worker: Rank top 10 items
        Worker->>Data: Save popular-item window
        Data->>DB: Begin database transaction
        Data->>DB: Save window boundaries
        Data->>DB: Save ranked items

        alt Save succeeds
            Data->>DB: Commit
            DB-->>Data: Commit confirmed
            Data-->>Worker: Window saved
        else Save fails
            Data->>DB: Roll back
            DB-->>Data: Save error
            Data-->>Worker: Persistence failure
            Worker->>Worker: Schedule ordered retry
        end
    end
```

A complete 1,000-scan window is first available at scan 1,000. A new checkpoint is then created
after each additional 500 accepted scans. Scan requests do not wait for checkpoint persistence.

## Retrieve Popular Items

```mermaid
sequenceDiagram
    participant Client as Checkout Client
    participant API as API Layer
    participant Analytics as Analytics Layer
    participant Data as Database Access
    participant DB as Database

    Client->>API: Request popular items
    API->>API: Validate requested limit
    API->>Analytics: Get latest popular items
    Analytics->>Data: Read latest window
    Data->>DB: Query latest persisted checkpoint
    DB-->>Data: Window and ranked items
    Data-->>Analytics: Popular-item result
    Analytics-->>API: Latest computed window
    API-->>Client: Successful popular-items response
```

The response represents the latest successfully persisted checkpoint. Scans recorded after that
checkpoint are not included until the next checkpoint has been computed and saved.

## Boundary Rules

- The API layer never reads or writes the database directly.
- The API layer forwards an accepted scan event; it does not decide whether a scan is accepted.
- The transaction layer owns basket state, pricing, completion, idempotency, and inventory
  consistency.
- The analytics layer owns the scan counter, rolling window, checkpoint schedule, ranking, and
  popular-item result.
- The database-access layer owns all database statements, transaction boundaries, and durable
  mappings.
- Results and errors return outward through explicit layer contracts; lower layers never depend
  on the API layer.
