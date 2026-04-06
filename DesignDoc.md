# FlexQL Design Document

Repository Link: `https://github.com/Salim786p/FlexQL.git`

## 1. Project Overview

FlexQL is a simplified SQL-like database driver implemented entirely in C and C++. The system follows a client-server architecture:

- the server accepts TCP connections, parses and executes SQL-like commands, and manages persistent storage
- the client library exposes the required opaque-handle API
- the REPL is a thin interactive shell built on top of the same client API

The main design goal was to build a system that is correct, persistent, and reasonably fast under insert-heavy workloads while staying small enough to understand and maintain.

## 2. Supported Functionality

The current implementation supports:

- `CREATE DATABASE`
- `USE`
- `SHOW DATABASES`
- `SHOW TABLES`
- `CREATE TABLE`
- `INSERT INTO ... VALUES (...)`
- `SELECT`
- `WHERE` with a single condition
- `INNER JOIN`

Delete and drop operations are not central to the current project design.

## 3. High-Level Design Decisions

The main design decisions taken in this project are:

- row-oriented storage in memory
- persistent per-table binary files on disk
- a primary-key hash index for fast equality lookup
- an LRU query-result cache
- a thread-per-client server model
- per-table reader-writer locking for safe concurrency
- a compact insert path that avoids unnecessary conversions during persistence
- a transparent binary insert protocol between client and server for lower request overhead

These choices were made to balance correctness, simplicity, and performance.

## 4. How the Data Is Stored

### 4.1 In-Memory Representation

Each database owns a map of tables:

- `Database`
  - `unordered_map<string, unique_ptr<Table>> tables_`

Each table stores:

- schema metadata as `vector<ColumnDef>`
- in-memory rows as `vector<Row>`
- a primary-key index as `unordered_map<string, size_t>`
- a column-name lookup map as `unordered_map<string, int>`

Each row stores:

- `vector<string> values`
- `deleted` flag
- `expiration` timestamp

This is a row-major design. It was chosen because the workload is dominated by row inserts and full-row retrievals rather than analytical column scans. A row-major layout keeps insert logic straightforward and matches the query style expected in the project.

### 4.2 On-Disk Layout

Each database is stored under:

```text
data/<DB_NAME>/
```

Inside each database directory:

- `schema.bin` stores table and column definitions
- each table has its own `<TABLE>.tbl` data file

The current table file format begins with:

- a 4-byte magic number
- a 4-byte column count

After that, each row is appended as:

- `deleted` flag
- `expiration` timestamp
- one length-prefixed cell per column

Null values are represented with a dedicated null marker rather than by storing the string `"NULL"` as ordinary text. Non-null cells are stored as raw string payloads. This design keeps the append path simple and avoids extra numeric encoding and decoding work during every insert.

### 4.3 Persistence Behavior

The server keeps tables in memory for fast reads, but writes new rows to disk as well. On startup:

- the schema is loaded from `schema.bin`
- each table is recreated in memory
- table rows are replayed from the `.tbl` file
- the primary-key index is rebuilt

This gives the project both persistence and fast steady-state query execution.

### 4.4 Backward Compatibility

The current loader can still read older table files produced by an earlier row format. This reduces migration pain while allowing the project to move to a simpler persistent format.

## 5. Indexing Method

The primary indexing structure is:

- `unordered_map<string, size_t>`

This maps primary-key value to row position in the table’s in-memory row vector.

### Why a hash index?

- equality lookup on the primary key is the most important indexed operation here
- average-case lookup is effectively O(1)
- the implementation is simple and integrates cleanly with row-oriented storage
- it is cheaper to maintain than a more complex tree structure for the current SQL subset

### How it is used

- on insert, the server checks the primary-key index to reject duplicates
- after a successful insert, the new row index is stored in the hash map
- `SELECT ... WHERE pk = value` uses direct primary-key lookup instead of scanning the table

This improves point-query performance significantly compared with a full scan.

## 6. Caching Strategy

FlexQL uses an LRU cache for repeated query results.

### Structure

The cache stores:

- the normalized SQL string as the key
- the `QueryResult`
- the list of tables referenced by that query

Implementation:

- `std::list` for LRU ordering
- `unordered_map` for O(1) lookup of cache entries
- reverse mapping from table name to dependent cached queries for targeted invalidation

### Why LRU?

LRU fits this project well because repeated interactive queries are often recent queries. It is simpler than LFU and more predictable for a student database server.

### Invalidation Strategy

When a table changes:

- all cached results depending on that table are invalidated

This keeps cached reads correct without clearing the entire cache on every insert.

## 7. Handling of Expiration Timestamps

Each row stores an expiration timestamp:

- `0` means no expiration
- non-zero values can be used to support time-based invalidation

In the current design:

- expiration metadata is stored both in memory and on disk
- rows remain structurally capable of supporting TTL/expiry behavior

This was kept in the row format so the project can support expiration cleanly without redesigning storage later.

## 8. Multithreading Design

The server uses a thread-per-client model:

- `accept()` receives a connection
- a detached worker thread handles that client session

### Concurrency Control

The following locking strategy is used:

- global database catalog protected by `shared_mutex`
- each table protected by its own `shared_mutex`
- cache protected by a standard `mutex`

### Why this design?

- multiple readers can access the same table concurrently
- inserts take exclusive access only on the affected table
- schema changes do not require locking unrelated tables
- the implementation stays relatively simple and predictable

### Thread Safety Behavior

- reads use shared locks where possible
- writes use exclusive locks
- cache operations are serialized internally
- database lookup and table lookup are separated to reduce unnecessary lock scope

This gives safe concurrency without requiring a much more complex execution engine.

## 9. Query Execution Decisions

The SQL executor is intentionally compact and focused on the required subset.

### Parsing Approach

The server uses lightweight SQL parsing logic tailored to the project’s supported commands. This keeps the codebase smaller than a full parser-generator approach and makes the supported grammar explicit.

### Insert Path

The insert path was optimized around a few principles:

- validate cheaply
- avoid exception-heavy numeric parsing
- move row batches where possible
- append to a buffered table file rather than issuing tiny writes repeatedly

The client library also uses a compact binary message format for insert requests. This lowers text parsing overhead on the server while keeping the public API unchanged.

### Select Path

For reads:

- full-table scans are used when necessary
- primary-key equality queries use direct index lookup
- joins use a hash-join style build/probe pattern instead of nested loops

This keeps the common query shapes faster without overcomplicating the system.

## 10. Additional Design Decisions

### 10.1 Case Handling

Database names, table names, and column names are normalized to uppercase internally. This simplifies lookup logic and reduces case-sensitivity bugs.

### 10.2 Result Formatting

Responses are sent over a length-prefixed protocol. Query results are encoded as:

- `OK`
- `ERROR`
- `RESULT`

This makes the wire protocol simple and easy for the client library to decode.

### 10.3 Client/Server Boundary

The public C API stays text-oriented from the user’s point of view, but the implementation is free to use a more efficient internal representation between client and server. This allowed performance improvements without changing the required API surface.

## 11. Compilation Instructions

Build the project from the repository root:

```bash
make
```

Artifacts:

- `bin/flexql-server`
- `bin/flexql-client`
- `bin/libflexql.a`

Requirements:

- `gcc`
- `g++`
- `make`
- POSIX-compatible environment with socket support

## 12. Execution Instructions

Start the server:

```bash
./bin/flexql-server
```

Start the client in another terminal:

```bash
./bin/flexql-client 127.0.0.1 9000
```

Example usage:

```sql
CREATE DATABASE APPDB;
USE APPDB;

CREATE TABLE USERS (
    ID INT PRIMARY KEY,
    NAME VARCHAR(64) NOT NULL,
    EMAIL TEXT,
    BALANCE DECIMAL
);

INSERT INTO USERS VALUES (1, 'Alice', 'alice@example.com', 1250.50);
SELECT * FROM USERS WHERE ID = 1;
SHOW TABLES;
```

## 13. Performance Results for Large Datasets

The following results were measured end-to-end on the current implementation using a clean data directory and the current client/server build.

### Insert-Heavy Workload

- 5,000 inserted rows
  - elapsed time: `434 ms`
  - throughput: about `11,520 rows/sec`

- 20,000 inserted rows
  - elapsed time: `1,790 ms`
  - throughput: about `11,173 rows/sec`

### Point Lookup by Primary Key

After loading 20,000 rows, an end-to-end primary-key equality query returned one row in about:

- `8.0 ms`

This includes client API, network round trip, server execution, and result decoding.

### Interpretation

The main strengths of the current design are:

- stable insert throughput
- durable row storage
- fast primary-key lookup compared with scanning
- acceptable performance for repeated read-heavy workloads with caching

The main remaining bottleneck is the in-memory row representation:

- rows are still stored as `vector<string>`
- this creates allocation and object-management overhead at very large scales

The next major improvement would likely come from moving to a more compact in-memory row arena or offset-based storage model.

## 14. Challenges and Tradeoffs

Several design tradeoffs shaped the final system:

- keeping persistence simple versus maximizing raw insert speed
- keeping the SQL subset easy to reason about versus implementing a more complex parser
- using a fast hash index for equality lookups instead of a heavier generalized tree index
- retaining an understandable thread-per-client model instead of switching to a more complex asynchronous design

The chosen design favors clarity, correctness, and solid baseline performance over aggressive specialization.

## 15. Conclusion

FlexQL is a compact database driver that demonstrates the core internal ideas behind a relational system:

- schema management
- persistent row storage
- indexing
- caching
- concurrent query execution
- client/server communication

The current design is general-purpose within the project scope, avoids file-specific hardcoding, and provides a clear base for future improvements in storage layout and large-scale performance.
