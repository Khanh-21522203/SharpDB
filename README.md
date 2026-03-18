# SharpDB

SharpDB is an embedded database engine written in C# (.NET 9). It is a learning-oriented project focused on storage engines, indexing, transactions, MVCC, and WAL-based durability.

## What Is Implemented

- Storage engine
  - Page-based storage manager
  - Buffered data I/O sessions
  - Header/metadata persistence for collections and schema
- Indexing
  - B+Tree primary index
  - Secondary indexes (unique and duplicate)
  - Public secondary index query APIs (exact match + unique range)
- Collection API
  - Schema-based collections
  - CRUD, scan, count, and range queries
  - Transaction-aware collection operations
- Transactions and concurrency
  - Isolation levels: `ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `Serializable`
  - MVCC visibility rules with commit/abort handling
  - Serializable range locking (coarse collection-range token)
  - Deadlock detection in lock manager
- Durability
  - WAL write path for begin/commit/abort + data changes
  - Recovery phases (analysis, redo, undo)
  - Checkpoint support and auto-checkpoint scheduling
- Relational helpers
  - Foreign key validation hooks (primary-key references)
  - Collection-level inner join APIs

## Current Limits

- No SQL parser or query optimizer (API-first engine).
- No distributed/replicated runtime.
- Serializable range locking is correctness-first and coarse-grained.
- Foreign key validation currently supports references to loaded target collections and their primary key field.

## Architecture At A Glance

Primary write flow:

`SharpDB -> CollectionManager -> TransactionBoundary -> Transaction/MVCC -> Index + Data Sessions -> Storage`

Durability flow:

`Transaction commit -> WAL append/flush -> MVCC commit -> optional auto-checkpoint`

Recovery flow:

`Startup -> WAL analysis -> REDO committed changes -> UNDO unfinished changes`

Core modules:

- `SharpDB/Engine`: collection orchestration, schema, transactions
- `SharpDB/Index`: B+Tree managers, node operations, index I/O sessions
- `SharpDB/Storage`: page/storage managers and buffered sessions
- `SharpDB/WAL`: log records, replay, checkpoint management

## Build

Requirements:

- .NET SDK 9.0+

Build commands:

```bash
dotnet build SharpDB.sln -v minimal
```

## Run

Run the demo app:

```bash
dotnet run --project SharpDB/SharpDB.csproj
```

Run the secondary-index example:

```bash
dotnet run --project TestSecondaryIndex/TestSecondaryIndex.csproj
```

## Test

Executable test harness:

```bash
dotnet run --project SharpDB.Tests/SharpDB.Tests.csproj
```

## Quick Start

```csharp
using SharpDB;
using SharpDB.Engine;

var db = new SharpDB.SharpDB("./mydb");

var schema = new Schema
{
    Fields =
    [
        new Field { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
        new Field { Name = "Name", Type = FieldType.String, MaxLength = 100 },
        new Field { Name = "Age", Type = FieldType.Int }
    ]
};

var users = await db.CreateCollectionAsync<User, long>("users", schema, u => u.Id);

await users.InsertAsync(new User { Id = 1, Name = "Alice", Age = 30 });
var user = await users.SelectAsync(1);

Console.WriteLine(user?.Name);

public sealed class User
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}
```

## Benchmark

SharpDB includes a CppColDB-style benchmark harness in `SharpDB.Benchmark/`.

What it provides:

- CLI-configurable workload (`--rows`, `--select-batch`, `--write-batch`)
- Warmup + measured iterations (`--warmup`, `--iters`)
- Tabular case stats (`avg_ms`, `p50`, `p95`, `min`, `max`, `qps`, `rows_out`)
- Both rollback and committed write cases
- Secondary index lookup (unique index on Email)

Benchmark cases:

| Case | What it measures |
|---|---|
| `read.count` | Full collection count via B+Tree leaf traversal |
| `read.pk_lookup_batch` | Batch of primary key point lookups |
| `read.range_scan_batch` | B+Tree range scan over a key window |
| `read.top_n` | Ordered B+Tree scan returning first 100 rows |
| `read.filter_scan` | Full collection scan with predicate filter |
| `read.secondary_lookup` | Point lookup via unique secondary index (Email) |
| `write.insert_rollback` | Insert batch then rollback (index write cost, no commit) |
| `write.insert_commit` | Insert batch (committed) + cleanup deletes (committed) |
| `write.update_rollback` | Update batch then rollback |
| `write.update_commit` | Update batch then commit |
| `write.delete_rollback` | Delete batch then rollback |

Run directly:

```bash
dotnet build SharpDB.Benchmark/SharpDB.Benchmark.csproj -c Release
dotnet run --project SharpDB.Benchmark/SharpDB.Benchmark.csproj --configuration Release -- \
  --rows 100000 \
  --warmup 3 \
  --iters 15 \
  --select-batch 5000 \
  --write-batch 1000 \
  --wal false
```

Use helper script:

```bash
./scripts/run_benchmark.sh
```

Override with environment variables:

```bash
ROWS=200000 WARMUP=4 ITERS=12 SELECT_BATCH=10000 WRITE_BATCH=2000 ./scripts/run_benchmark.sh
DB_PATH=/tmp/sharpdb_bench WAL=true ./scripts/run_benchmark.sh
```

Sample statistics (this machine, `rows=100000 warmup=3 iterations=15 select_batch=5000 write_batch=1000 wal=false`):

| Case | avg_ms | p50 | p95 | min | max | qps | rows_out | gc0/iter |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `read.count` | 0.708 | 0.467 | 1.816 | 0.241 | 3.449 | 1412.3 | 100000.0 | 0.00 |
| `read.pk_lookup_batch` | 5.801 | 5.759 | 6.193 | 5.330 | 6.639 | 172.4 | 5000.0 | 1.13 |
| `read.range_scan_batch` | 5.237 | 5.163 | 6.326 | 4.394 | 6.561 | 190.9 | 5000.0 | 0.60 |
| `read.top_n` | 0.131 | 0.083 | 0.274 | 0.080 | 0.624 | 7661.7 | 100.0 | 0.00 |
| `read.filter_scan` | 24.017 | 20.150 | 36.572 | 17.732 | 36.660 | 41.6 | 60000.0 | 2.87 |
| `read.secondary_lookup` | 15.681 | 16.341 | 18.468 | 12.595 | 18.622 | 63.8 | 5000.0 | 2.13 |
| `write.insert_rollback` | 19.925 | 17.745 | 32.240 | 13.536 | 40.802 | 50.2 | 1000.0 | 1.53 |
| `write.insert_commit` | 27.133 | 26.007 | 33.519 | 22.522 | 37.304 | 36.9 | 1000.0 | 2.00 |
| `write.update_rollback` | 29.585 | 30.515 | 34.118 | 24.892 | 34.650 | 33.8 | 1000.0 | 2.47 |
| `write.update_commit` | 26.046 | 22.171 | 48.707 | 14.301 | 48.876 | 38.4 | 1000.0 | 1.53 |
| `write.delete_rollback` | 4.168 | 4.211 | 7.356 | 2.099 | 8.998 | 239.9 | 119.4 | 0.27 |

Per-operation cost (both lookup cases use `select_batch=5000`):

| Case | µs/op | Explanation |
|---|---:|---|
| `read.pk_lookup_batch` | ~1.2µs | Long key (8 B), small B+Tree nodes (~1.4 KB each) |
| `read.secondary_lookup` | ~3.1µs | String key (255 B), large B+Tree nodes (~17 KB each) → fewer nodes fit in LRU cache |

Notes on performance:

- All write cases maintain the secondary Email index (255-byte string B+Tree), which dominates write cost.
- `write.insert_commit` inserts 1000 records (committed), then deletes the same 1000 records (committed) to keep the dataset stable. Both the insert and delete transactions include full secondary-index maintenance (insert + remove Email B+Tree entries), which is measured correctly here.
- **B+Tree node buffer pool** (~50–60% write improvement): index nodes accumulate in an in-memory session and flush once at transaction commit/rollback. `PatchPointer` on `InternalNode`/`LeafNode` resolves temporary negative-position pointers in deferred dirty nodes during the single final flush, eliminating thousands of intermediate disk writes per write batch.
- **`ISerializer<T>.SerializeTo(T, Span<byte>)`** (~40% write improvement on top of buffer pool): all serializers write directly into node's `_data` buffer via `BinaryPrimitives`, eliminating one `byte[]` allocation per key/value write. Also fixes `Pointer.FillBytes` and `TreeNode.KeyCount` setter to use `BinaryPrimitives`.
- **`StringSerializer.Deserialize` SIMD null-trim** (~50% secondary lookup improvement): `LastIndexOfAnyExcept((byte)0)` span scan replaces `TrimEnd('\0')`, eliminating the second string allocation per B+Tree key comparison.
- **Zero-copy scan deserialization** (~53% filter_scan improvement): `DBObject.RawData`/`DataOffset` exposes the raw page buffer directly. `CollectionManager.ScanAsync` passes it to `IObjectSerializer.Deserialize<T>(byte[], int offset)`, eliminating the 60K × ~370-byte `new byte[DataSize]` copy that previously occurred per record in every full scan.
- **Compiled expression-tree deserializer** (~70% filter_scan gc0 reduction): `BinaryObjectSerializer.Deserialize<T>` builds a `Func<byte[], int, T>` per `(Type, Schema)` using `System.Linq.Expressions`. The compiled lambda creates instances via `new T()` (no `Activator` reflection), reads each field at a compile-time-constant offset, and sets properties directly — eliminating boxing of `int`/`long`/`DateTime` fields and `PropertyInfo.SetValue` overhead across all 60K records per scan.
- Remaining `gc0/iter` on `read.filter_scan` (2.87) is driven by the 60K Email string allocations (~260 bytes each, ~15.6 MB/iter) — unavoidable since C# strings are heap-allocated.

## Repository Layout

```text
.
├── SharpDB/                # core engine/runtime
├── SharpDB.Tests/          # executable regression test harness
├── SharpDB.Benchmark/      # benchmark harness
├── TestSecondaryIndex/     # small example project
├── scripts/                # utility scripts (benchmark runner)
└── tasks/                  # planning and design notes
```

## Contributing

Contributions are welcome. Keep changes minimal, test-backed, and aligned with current architecture direction.

Typical local validation:

```bash
dotnet build SharpDB.sln -v minimal
dotnet run --project SharpDB.Tests/SharpDB.Tests.csproj
```
