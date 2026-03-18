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
- Rollback-based write cases for stable repeated runs

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

| Case | avg_ms | p50 | p95 | min | max | qps | rows_out |
|---|---:|---:|---:|---:|---:|---:|---:|
| `read.count` | 8.998 | 8.793 | 10.580 | 7.794 | 10.594 | 111.1 | 100000.0 |
| `read.pk_lookup_batch` | 17.987 | 17.007 | 26.167 | 13.310 | 36.448 | 55.6 | 5000.0 |
| `read.range_scan_batch` | 15.158 | 10.143 | 34.740 | 6.801 | 36.534 | 66.0 | 5000.0 |
| `read.filter_scan` | 201.521 | 196.185 | 246.366 | 188.603 | 246.936 | 5.0 | 60000.0 |
| `write.insert_rollback` | 33.771 | 31.836 | 46.689 | 26.396 | 62.550 | 29.6 | 1000.0 |
| `write.update_rollback` | 51.886 | 51.648 | 59.270 | 45.939 | 59.872 | 19.3 | 1000.0 |
| `write.delete_rollback` | 4.554 | 4.106 | 7.063 | 3.296 | 7.596 | 219.6 | 104.3 |

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
