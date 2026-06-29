# async-redis

A high-performance asynchronous **In-Memory NoSQL database** written in **Python**. Implements a subset of the canonical **RESP (Redis Serialization Protocol)**, provides non-blocking data persistence, and supports multiple complex data structures.

---

# Architecture & Components

### Network Layer

* Asynchronous TCP server built on `asyncio.StreamReader` / `asyncio.StreamWriter`.
* Supports **command pipelining**, allowing multiple RESP commands to be processed from a single read buffer.

### RESP Parser

* Streaming implementation of the Redis Serialization Protocol (`+`, `-`, `:`, `$`, `*`).
* Built on top of `io.BytesIO` for efficient incremental parsing.

### Storage Engine

* In-memory key-value repository.
* Thread-safe access interface for concurrent operations.

### Persistence (AOF Everysec)

* Write operations are appended to an **Append-Only File (AOF)** through a non-blocking in-memory buffer.
* Disk synchronization (`fsync`) is handled by a background worker exactly once per second, preventing the main event loop from blocking.

### Log Compaction (BGREWRITEAOF)

* Background asynchronous AOF rewrite.
* The current in-memory dataset is serialized into a temporary file using `asyncio.to_thread()`, minimizing Event Loop overhead.

### Concurrent Blocking Operations

* `WaitManager` coordinates waiting clients for blocking commands (`BLPOP`, `BRPOP`, `XREAD BLOCK`).
* Built on `asyncio.Event`, eliminating CPU-intensive polling.

### Reactive Pub/Sub

* Message routing service for publish/subscribe channels.
* Automatically removes disconnected subscribers when TCP connections are closed.

### TTL Manager

* Background worker responsible for active expiration of keys.
* Implements Redis-style **random sampling**, stopping cleanup when expired keys account for less than **25%** of the sampled set.

---

# Supported Data Types & Commands

## System & Transactions

### Transactions

* `MULTI`
* `EXEC`
* `DISCARD`

Provides atomic execution of queued commands.

### Persistence

* `BGREWRITEAOF`

Performs asynchronous background log compaction.

---

## Strings (`RedisString`)

```text
SET key value [EX ttl]
```

Store a string value with an optional expiration time.

```text
GET key
```

Retrieve a value.

```text
DEL key [key ...]
```

Delete one or more keys.

```text
INCR key
```

Atomically increment an integer value.

---

## Lists (`RedisList`)

```text
LPUSH key item [item ...]
RPUSH key item [item ...]
```

Insert elements at the head or tail of a doubly-ended queue.

```text
LPOP key
RPOP key
```

Remove and return an element from either end.

```text
BLPOP key timeout
BRPOP key timeout
```

Blocking pop with timeout support.

---

## Hashes (`RedisHash`)

```text
HSET key field value [field value ...] [EX ttl]
```

Store field-value pairs inside a hash.

```text
HGET key field
HDEL key field [field ...]
```

Read and delete hash fields.

```text
HEXISTS key field
HLEN key
```

Check field existence and count stored fields.

```text
HGETALL key
HKEYS key
HVALS key
```

Retrieve the entire hash, its keys, or its values.

---

## Streams (`RedisStream`)

```text
XADD key id field value [field value ...]
```

Append a stream entry.

Supports:

* auto-generated IDs (`*`)
* monotonic `ms-seq` ID validation

```text
XRANGE key start end
```

Read a range of entries.

Supports `-` and `+` anchors.

```text
XREAD STREAMS key id [BLOCK ms]
```

Read entries with IDs greater than the specified one.

Supports optional blocking mode.

---

## Pub/Sub

```text
SUBSCRIBE channel
```

Subscribe to a message channel.

```text
PUBLISH channel message
```

Publish a message to all subscribers.

---

# Benchmark

## Benchmark Configuration

Benchmarks were performed using the official **redis-benchmark** utility with:

* Persistent TCP connections (`keep-alive`)
* 50 concurrent clients
* 100,000 requests per command

```bash
redis-benchmark -p 6379 -c 50 -n 100000 -t set,get,lpush
```

---

# Throughput

| Command | Operation                   |           Performance |
| ------- | --------------------------- | --------------------: |
| GET     | In-memory read              | **67,069.08 req/sec** |
| SET     | Memory write + buffered AOF | **64,557.78 req/sec** |
| LPUSH   | List modification           | **57,405.28 req/sec** |

---

# Latency

## SET

| Metric       |        Value |
| ------------ | -----------: |
| Average      | **0.770 ms** |
| Minimum      | **0.320 ms** |
| Median (P50) | **0.759 ms** |
| P99          | **1.015 ms** |
| Maximum      | **4.959 ms** |

---

## GET

| Metric       |         Value |
| ------------ | ------------: |
| Average      |  **0.738 ms** |
| Minimum      |  **0.312 ms** |
| Median (P50) |  **0.711 ms** |
| P99          |  **1.383 ms** |
| Maximum      | **15.343 ms** |

---

## LPUSH

| Metric       |        Value |
| ------------ | -----------: |
| Average      | **0.866 ms** |
| Minimum      | **0.320 ms** |
| Median (P50) | **0.863 ms** |
| P99          | **1.023 ms** |
| Maximum      | **3.295 ms** |
