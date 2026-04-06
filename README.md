# FlexQL – A Flexible SQL-like Database Driver

> **Repository structure, build instructions, design rationale, and performance notes.**

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Folder Structure](#folder-structure)
3. [How to Build and Run](#how-to-build-and-run)
4. [Supported SQL Commands](#supported-sql-commands)
5. [Design Document](#design-document)
   - Storage Design
   - Indexing
   - Caching Strategy
   - Expiration Timestamps
   - Multithreading Design
6. [API Reference](#api-reference)
7. [Performance Notes](#performance-notes)
8. [Example Session](#example-session)

---

## Project Overview

FlexQL is a simplified, client-server SQL-like database system written entirely in **C/C++** (C++17 server, C99 client library). It supports a meaningful subset of relational operations:

- `CREATE TABLE`
- `INSERT INTO … VALUES`
- `SELECT … FROM … [WHERE …]`
- `SELECT … FROM … INNER JOIN … ON … [WHERE …]`

The server is a multithreaded TCP daemon. The client is a thin C library plus an interactive REPL that mirrors the feel of `sqlite3` or `psql`.

---

## Folder Structure

```
flexql/
├── include/
│   └── flexql.h              ← Public C API (the only header users need)
├── src/
│   ├── server/
│   │   └── server.cpp        ← Entire server: parser, storage, cache, network
│   └── client/
│       ├── flexql_client.c   ← Client library implementing flexql_*.h
│       └── repl.c            ← Interactive terminal (REPL)
├── bin/                      ← Created by make; holds compiled binaries
├── Makefile
└── README.md
```

---

## How to Build and Run

### Prerequisites

| Tool | Minimum Version |
|------|----------------|
| g++ / gcc | 7+ (C++17 support) |
| make | any |
| Linux / macOS | (POSIX sockets required) |

### Build Everything

```bash
# From the project root (where Makefile lives)
make
```

This produces:
- `bin/flexql-server`   – the database server
- `bin/flexql-client`   – the interactive REPL
- `bin/libflexql.a`     – static library (link your own programs against this)

### Start the Server

```bash
# Default port 9000
./bin/flexql-server

# Custom port
./bin/flexql-server 5432
```

### Start the REPL Client

```bash
# In a separate terminal
./bin/flexql-client 127.0.0.1 9000
```

You should see:
```
Connected to FlexQL server at 127.0.0.1:9000
Type .help for help, .exit to quit.

flexql>
```

### Use the C Library in Your Own Program

```c
#include "flexql.h"   // only this header is needed

// link with:  gcc yourfile.c -Lbin -lflexql -o yourprog
```

---

## Supported SQL Commands

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column1 TYPE [PRIMARY KEY] [NOT NULL],
    column2 TYPE [NOT NULL],
    ...
);
```

Supported types: `INT`, `DECIMAL`, `VARCHAR`, `TEXT`, `DATETIME`.

### INSERT

```sql
INSERT INTO table_name VALUES (val1, val2, ...);

-- With an absolute expiration (Unix timestamp):
INSERT INTO table_name VALUES (val1, val2, ...) EXPIRE 1800000000;

-- With a relative TTL in seconds (expires 60 s from now):
INSERT INTO table_name VALUES (val1, val2, ...) TTL 60;
```

### SELECT

```sql
-- All columns
SELECT * FROM table_name;

-- Specific columns
SELECT col1, col2 FROM table_name;

-- With a WHERE clause (single equality condition only)
SELECT * FROM table_name WHERE column = value;
```

### INNER JOIN

```sql
SELECT *
FROM tableA
INNER JOIN tableB
ON tableA.column = tableB.column;

-- With WHERE
SELECT *
FROM tableA
INNER JOIN tableB
ON tableA.id = tableB.id
WHERE tableA.name = 'Alice';
```

> **Restriction**: Only one WHERE condition is supported. AND/OR are not supported.

---

## Design Document

### 1. Storage Design – Row-Major Format

**Choice**: Row-major storage.

Each table is represented as a `std::vector<Row>` where every `Row` is a `std::vector<std::string>` (all column values stored as strings).

**Why row-major?**
This workload is OLTP-style: clients issue `INSERT` of single rows and `SELECT` that retrieves full rows. Row-major format means:
- A single INSERT appends one contiguous object to the vector → O(1) amortised.
- A full-row fetch (SELECT *) reads from a single cache line run.
- Column-major (columnar) storage would suit analytics / aggregation workloads where only a few columns are scanned. That is not this project's primary use-case.

**Schema information** is stored as `std::vector<ColumnDef>` alongside the data vector, inside the `Table` class. Each `ColumnDef` holds name (upper-cased), type, `primary_key`, and `not_null` flags. This schema is consulted on every `INSERT` for type validation and on every `SELECT` to resolve column names.

**Tables** are owned by the `Database` object, stored in an `std::unordered_map<string, unique_ptr<Table>>`. The map is keyed by the upper-cased table name for case-insensitive lookup.

---

### 2. Indexing

**Structure**: `std::unordered_map<std::string, size_t>` — maps primary-key value (as a string) to the row index inside the `vector<Row>`.

**Why unordered_map?**
- O(1) average-case lookup vs. O(log n) for a B-tree.
- For equality queries (`WHERE id = 42`) — the only supported single-condition query — hashing is optimal.
- For the ~10 M row benchmark, this avoids a full scan when the WHERE column is the primary key.

**Building the index**: every `INSERT` appends a row and inserts `{pk_value → index}` into the hash map. When rows are soft-deleted (expiration), the background cleanup thread rebuilds the index for affected tables.

---

### 3. Caching Strategy – LRU

**Algorithm**: Least-Recently-Used (LRU) with a capacity of **1 000 entries**.

**Implementation**:
- A `std::list<Entry>` stores entries in MRU-to-LRU order.
- An `std::unordered_map<key, list_iterator>` provides O(1) lookup.
- On a cache hit the entry is spliced to the front of the list.
- On a cache miss the entry is inserted at the front; if the list exceeds capacity, the back element is evicted.

**Cache key**: the upper-cased, trimmed SQL string.

**Cache invalidation** (correctness guarantee):
- Each cache entry records which tables it depends on (`vector<string> tables`).
- A reverse index `unordered_map<table_name, set<cache_keys>>` allows O(|affected entries|) targeted invalidation.
- On every `INSERT`, `cache().invalidate(table_name)` is called, evicting all cached SELECT results that referenced that table.

**Why LRU over LFU?**
For a student database application the most recently-issued SELECT queries are the most likely to be repeated (e.g. a developer running the same query repeatedly). LRU handles this well. LFU would be better for long-running servers with stable hot sets, but adds implementation complexity.

---

### 4. Expiration Timestamps

**Storage**: every `Row` carries a `time_t expiration` field (0 = never expires).

**Setting expiration**:
```sql
INSERT INTO logs VALUES (1, 'event') EXPIRE 1710000000;   -- absolute Unix ts
INSERT INTO sessions VALUES (42, 'tok') TTL 3600;          -- relative: now+3600s
```

**Enforcement**:
1. **Read-time filtering**: `SELECT` and `get_all_rows()` compare `row.expiration` against `time(nullptr)` and skip rows that have expired. No stale data is ever returned.
2. **Background compaction**: A daemon thread (`expiration_thread`) wakes every **5 seconds**, marks expired rows as `deleted = true`, and rebuilds the primary-key index for each table where rows changed. This keeps memory from growing unboundedly.

---

### 5. Multithreading Design

**Server model**: one thread per client connection (`std::thread` detached). This is simple, correct, and performant up to hundreds of simultaneous clients. For thousands of clients a thread-pool or async I/O model would be preferred, but is beyond this project scope.

**Concurrency control**:

| Resource | Lock Type | Rationale |
|----------|-----------|-----------|
| Schema map (`tables_` inside `Database`) | `std::shared_mutex` | Many threads can query different tables concurrently; only `CREATE TABLE` needs exclusive access. |
| Individual `Table` data | `std::shared_mutex` per table | Multiple `SELECT` threads can read the same table simultaneously (shared lock). `INSERT` and expiration cleanup take an exclusive lock on that table only. |
| LRU Cache | `std::mutex` | Writes (put/invalidate) mutate list pointers; must be exclusive. A `shared_mutex` would help for get-only paths but was not necessary for correctness. |

**Race conditions prevented**:
- A thread reading rows always holds at least a shared lock; an inserting thread cannot corrupt the vector while a reader is iterating.
- The `pk_index_` map is only written under an exclusive lock.
- Cache invalidation is serialised by the cache's own mutex, separate from the table lock, so no cross-lock ordering issue.

**Deadlock avoidance**: the code never holds two table locks simultaneously. The JOIN executor acquires the left-table lock, releases it (through `get_all_rows()` returning a snapshot), then acquires the right-table lock. No circular waits.

---

## API Reference

```c
#include "flexql.h"

/* Open a connection to the server */
int flexql_open(const char *host, int port, FlexQL **db);
//   Returns FLEXQL_OK (0) or FLEXQL_ERROR (1).

/* Close the connection and free the handle */
int flexql_close(FlexQL *db);

/* Execute an SQL statement.
 * For SELECT, 'callback' is invoked once per returned row:
 *   int callback(void *data, int columnCount,
 *                char **values, char **columnNames);
 *   Return 0 to continue, 1 to abort.
 * On error, *errmsg is set; free it with flexql_free().
 */
int flexql_exec(FlexQL *db, const char *sql,
                int (*callback)(void *, int, char **, char **),
                void *arg, char **errmsg);

/* Free memory allocated by the library */
void flexql_free(void *ptr);

/* Error codes */
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1
```

---

## Performance Notes

Designed with a **~10 million row** benchmark in mind:

| Technique | Benefit |
|-----------|---------|
| Row-major `vector<Row>` | Sequential appends are O(1) amortised; great cache locality on full-row reads |
| Hash-based primary index | O(1) PK lookup; avoids full table scan for `WHERE pk = X` |
| LRU query cache | Repeated identical SELECTs served from memory without re-scanning rows |
| Shared reader lock | Multiple SELECT threads run concurrently on the same table |
| Hash-join for INNER JOIN | O(N+M) join instead of O(N×M) nested-loop join |
| Soft-delete with deferred compaction | Expired-row removal is batched every 5 s, not inline during inserts |

**Expected benchmark results** (rough estimates on a modern laptop):

| Operation | Expected throughput |
|-----------|-------------------|
| INSERT (single-threaded) | ~200 000–500 000 rows/sec |
| SELECT * (no WHERE, 10 M rows, cold cache) | ~1–3 sec |
| SELECT with PK WHERE (cached after first run) | < 1 ms |

---

## Example Session

```
$ ./bin/flexql-server 9000 &
FlexQL server started on port 9000

$ ./bin/flexql-client 127.0.0.1 9000
Connected to FlexQL server at 127.0.0.1:9000

flexql> CREATE TABLE STUDENT(
     ->   ID INT PRIMARY KEY NOT NULL,
     ->   FIRST_NAME TEXT NOT NULL,
     ->   LAST_NAME TEXT NOT NULL,
     ->   EMAIL TEXT NOT NULL
     -> );
OK

flexql> INSERT INTO STUDENT VALUES (1,'John','Doe','john@gmail.com');
OK

flexql> INSERT INTO STUDENT VALUES (2,'Alice','Smith','alice@gmail.com');
OK

flexql> SELECT * FROM STUDENT;
ID = 1
FIRST_NAME = John
LAST_NAME = Doe
EMAIL = john@gmail.com

ID = 2
FIRST_NAME = Alice
LAST_NAME = Smith
EMAIL = alice@gmail.com

flexql> SELECT * FROM STUDENT WHERE ID = 2;
ID = 2
FIRST_NAME = Alice
LAST_NAME = Smith
EMAIL = alice@gmail.com

flexql> .exit
Connection closed
```

---

## Notes

- The implementation is **in-memory only**. Data is lost when the server is stopped.
- No `AND`/`OR` in WHERE clauses (per specification).
- The REPL sends a query when it detects a trailing `;`. Multi-line queries are supported.
- The `TTL` extension is **not** standard SQL; it is a FlexQL extension for row expiration.
