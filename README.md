# FlexQL

FlexQL is a lightweight client-server SQL-like database system written in C and C++. It provides a small relational feature set, persistent on-disk storage, a multithreaded TCP server, and a C client API that can be used from both programs and the included interactive REPL.

The implementation is designed around a few core goals:
- keep inserts durable and simple
- support concurrent clients safely
- speed up repeated reads through indexing and caching
- remain easy to build and reason about without external database libraries

## Features

- Persistent databases stored under `data/`
- `CREATE DATABASE`
- `USE`
- `SHOW DATABASES`
- `SHOW TABLES`
- `CREATE TABLE`
- `INSERT INTO ... VALUES (...)`
- `INSERT INTO ... VALUES (...) EXPIRE <unix_timestamp>`
- `INSERT INTO ... VALUES (...) TTL <seconds>`
- `SELECT`
- `WHERE` with a single condition
- `INNER JOIN`
- Primary-key indexing with a B+ tree
- Query-result caching
- Multithreaded server

## Repository Layout

```text
.
├── flexql.h
├── flexql_client.c
├── flexql_server.cpp
├── repl.c
├── Makefile
├── README.md
└── DesignDoc.md
```

## Build

From the project root:

```bash
make
```

This builds:
- `bin/flexql-server`
- `bin/flexql-client`
- `bin/libflexql.a`

## Run

Start the server:

```bash
./bin/flexql-server
```

Start the client in another terminal:

```bash
./bin/flexql-client 127.0.0.1 9000
```

## Example Session

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
INSERT INTO USERS VALUES (2, 'Bob', 'bob@example.com', 980.00);

SELECT * FROM USERS;
SELECT NAME, BALANCE FROM USERS WHERE ID = 1;
SHOW TABLES;
```

## Public API

The client library exposes the required opaque-handle API in [`flexql.h`](/home/salimansari/Desktop/FlexQL/flexql.h):

- `flexql_open`
- `flexql_close`
- `flexql_exec`
- `flexql_free`

The REPL in [`repl.c`](/home/salimansari/Desktop/FlexQL/repl.c) is built on top of the same API.

## Design Summary

FlexQL uses row-oriented in-memory storage and persistent table files on disk. Each table keeps:
- schema metadata
- in-memory rows for query execution
- a primary-key B+ tree for exact and ordered lookups
- a per-table shared mutex for concurrent readers and writers

The server also keeps:
- an in-memory catalog of databases and tables
- an LRU cache for repeated `SELECT` results
- a thread-per-connection execution model
- hash-based structures where they fit naturally, such as column lookup maps, cache maps, and join build tables

The current on-disk row format stores:
- a table header with a magic number and column count
- one record per row
- row metadata plus length-prefixed string cell values

This keeps persistence simple while avoiding extra conversion work during inserts.

Rows can also carry expiration metadata. The server accepts both `EXPIRE` and `TTL` forms on insert, stores the absolute expiration timestamp, and filters expired rows during reads.

## Performance Snapshot

Recent end-to-end measurements on the current implementation:
- 5,000 inserted rows: `434 ms`, about `11,520 rows/sec`
- 20,000 inserted rows: `1,790 ms`, about `11,173 rows/sec`
- point lookup by primary key after loading 20,000 rows: about `8.0 ms` end-to-end through the client API

These numbers were measured on a clean data directory with the current server and client build.

## Notes

- Table data is persistent across server restarts.
- The implementation does not rely on external database libraries.
- `SHOW DATABASES`, `SHOW TABLES`, and `USE` are supported as required.
- Delete/drop functionality is intentionally not a core part of the project design.

## More Detail

See [`DesignDoc.md`](/home/salimansari/Desktop/FlexQL/DesignDoc.md) for the full design document, including storage, indexing, caching, expiration handling, concurrency, compilation, execution, and performance discussion.
