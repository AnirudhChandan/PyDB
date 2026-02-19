# PyDB: Relational B-Tree Storage Engine

PyDB is a custom, disk-based transactional database engine built entirely from scratch in Python.

It bypasses OS-level file caching to implement custom memory paging, raw binary serialization, and a recursive $O(\log N)$ data structure. It was built to deeply understand the low-level mechanics of relational databases like SQLite and PostgreSQL, specifically focusing on disk I/O optimization, indexing, and ACID compliance.

## Core Architecture

Instead of relying on high-level abstractions, PyDB interacts directly with the file system using raw byte manipulation (`struct` packing).

1. **The Pager (Memory Manager):** Divides the database file into strict `8192-byte` (8KB) pages. This ensures that memory reads/writes align with optimal disk block sizes.
2. **Dual B-Tree Indexing:** - **Primary Index:** Stores the full row data (291 bytes) clustered by a 4-byte Integer ID.
   - **Secondary Index:** Stores a 4-byte Email Hash mapped to the Primary ID, turning an $O(N)$ table scan into a sub-millisecond $O(\log N)$ lookup.
3. **Write-Ahead Log (WAL):** Ensures ACID compliance. All transactions are written to an append-only log and flushed to disk via `os.fsync()` before the B-Tree is modified, completely eliminating the Dual-Write Problem during unexpected power failures.

## Benchmarks & Performance

Hardware limitations (SSD synchronous write speeds) and algorithmic efficiency were measured using an automated benchmark suite (`benchmark.py`).

**Test Parameters:** 10,000 synthetic rows inserted. 1,000 random reads executed.

| Metric                   | Result             | Note                                                                                    |
| :----------------------- | :----------------- | :-------------------------------------------------------------------------------------- |
| **Write Throughput**     | `2,452 Ops/Sec`    | Constrained by strictly unbatched `os.fsync()` calls ensuring absolute data durability. |
| **Read Latency (Index)** | `0.295 ms / query` | Near-instant retrieval traversing two separate B-Trees.                                 |
| **Data Integrity**       | `1000/1000`        | Zero orphan indexes or corrupted pointers.                                              |
| **Disk Footprint**       | `7.37 MB Total`    | 5.59MB Primary, 0.13MB Secondary Index, 1.66MB WAL.                                     |

## Deep Dive: The Node Splitting Mechanic

A standard list appends data. PyDB uses a B-Tree that balances itself. When an 8KB Leaf Node fills its capacity, the engine triggers a split:

1. Allocates a new 8KB page via the Pager.
2. Migrates the upper 50% of the byte-array to the new page.
3. Updates the Internal Node (Parent) with the new boundary keys and child pointers.
4. Maintains sorting across the physical disk.

## Quick Start

**Prerequisites:** Python 3.8+ (No external dependencies required).

**Supported CLI Commands**
`db > insert 1 anirudh anichandan124@gmail.com
db > where email=anichandan124@gmail.com
db > .exit`
