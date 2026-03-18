using System.Diagnostics;
using System.Globalization;
using SharpDB.Configuration;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Engine;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Error()
    .WriteTo.Console()
    .CreateLogger();

try
{
    var cfg = ParseArgs(args);
    var ownsDbPath = string.IsNullOrWhiteSpace(cfg.DbPath);
    var dbPath = ownsDbPath
        ? Path.Combine(Path.GetTempPath(), $"sharpdb-bench-{Guid.NewGuid():N}")
        : cfg.DbPath!;

    var engineConfig = new EngineConfig
    {
        PageSize = 8192,
        BTreeDegree = 64,
        EnableWAL = cfg.EnableWal,
        WALAutoCheckpoint = false,
        SyncOnWrite = false, // Skip per-write fsync for benchmark throughput
        Cache = new CacheConfig { PageCacheSize = 4000 }
    };

    try
    {
        Directory.CreateDirectory(dbPath);

        using var db = new SharpDB.SharpDB(dbPath, engineConfig);
        var people = await db.CreateCollectionAsync<BenchmarkPerson, long>("people", PersonSchema(), p => p.Id);

        // Unique secondary index on Email — one record per email, built incrementally with inserts.
        // (Non-unique secondary indexes are supported but have a 100-value-per-key cap in BinaryListSerializer.)
        await people.CreateSecondaryIndexAsync<string>("Email", p => p.Email, isUnique: true);

        await PopulateBaseDataAsync(people, cfg.Rows);
        await db.FlushAsync();

        var selectCursor = 0;
        var rangeCursor = 0;
        var updateCursor = 0;
        var deleteCursor = 0;
        var ageLookupCursor = 0;
        var commitUpdateCursor = 0;
        long topNCursor = 1L;
        long nextInsertId = cfg.Rows + 1L;
        // Committed inserts start far above the rollback range to avoid ID conflicts.
        long nextCommitInsertId = (long)cfg.Rows * 10L + 1L;

        var cases = new List<BenchCase>
        {
            new("read.count", async () =>
            {
                var count = await people.CountAsync();
                return count;
            }),
            new("read.pk_lookup_batch", async () =>
            {
                long hits = 0;
                for (var i = 0; i < cfg.SelectBatch; i++)
                {
                    var id = (selectCursor + i) % cfg.Rows + 1;
                    var person = await people.SelectAsync(id);
                    if (person != null)
                        hits++;
                }

                selectCursor = (selectCursor + cfg.SelectBatch) % cfg.Rows;
                return hits;
            }),
            new("read.range_scan_batch", async () =>
            {
                long rowsOut = 0;
                var window = Math.Min(cfg.SelectBatch, cfg.Rows);
                var min = (rangeCursor % cfg.Rows) + 1;
                var max = Math.Min(cfg.Rows, min + window - 1);

                await foreach (var _ in people.RangeQueryAsync(min, max))
                    rowsOut++;

                rangeCursor = (rangeCursor + window) % cfg.Rows;
                return rowsOut;
            }),
            new("read.top_n", async () =>
            {
                // Fetch a fixed 100-row window via B+Tree range — tests ordered index scan.
                const int n = 100;
                long rowsOut = 0;
                var min = topNCursor;
                var max = min + n - 1;
                topNCursor = (topNCursor + n - 1) % cfg.Rows + 1;

                await foreach (var _ in people.RangeQueryAsync(min, max))
                    rowsOut++;

                return rowsOut;
            }),
            new("read.filter_scan", async () =>
            {
                long rowsOut = 0;
                await foreach (var person in people.ScanAsync())
                {
                    if (person.Age >= 40)
                        rowsOut++;
                }
                return rowsOut;
            }),
            new("read.secondary_lookup", async () =>
            {
                // Batch of point lookups via unique secondary index on Email.
                // Same batch size as pk_lookup_batch to make per-op cost directly comparable.
                long hits = 0;
                for (var i = 0; i < cfg.SelectBatch; i++)
                {
                    var id = (ageLookupCursor + i) % cfg.Rows + 1;
                    var email = $"person{id}@example.com";
                    var person = await people.SelectBySecondaryIndexAsync<string>("Email", email);
                    if (person != null) hits++;
                }
                ageLookupCursor = (ageLookupCursor + cfg.SelectBatch) % cfg.Rows;
                return hits;
            }),
            new("write.insert_rollback", async () =>
            {
                await using var tx = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted);
                for (var i = 0; i < cfg.WriteBatch; i++)
                    await people.InsertAsync(BuildPerson(nextInsertId + i), tx);

                nextInsertId += cfg.WriteBatch;
                await tx.RollbackAsync();
                return cfg.WriteBatch;
            }),
            new("write.insert_commit", async () =>
            {
                // Insert batch (committed), then delete same rows (committed) to keep dataset stable.
                var startId = nextCommitInsertId;
                nextCommitInsertId += cfg.WriteBatch;

                await using var ins = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted);
                for (var i = 0; i < cfg.WriteBatch; i++)
                    await people.InsertAsync(BuildPerson(startId + i), ins);
                await ins.CommitAsync();

                await using var del = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted);
                for (var i = 0; i < cfg.WriteBatch; i++)
                    await people.DeleteAsync(startId + i, del);
                await del.CommitAsync();

                return cfg.WriteBatch;
            }),
            new("write.update_rollback", async () =>
            {
                await using var tx = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted);
                long updated = 0;
                for (var i = 0; i < cfg.WriteBatch; i++)
                {
                    var id = (updateCursor + i) % cfg.Rows + 1;
                    var person = await people.SelectAsync(id, tx);
                    if (person == null)
                        continue;

                    person.Age++;
                    person.Score += 1.0;
                    await people.UpdateAsync(person, tx);
                    updated++;
                }

                updateCursor = (updateCursor + cfg.WriteBatch) % cfg.Rows;
                await tx.RollbackAsync();
                return updated;
            }),
            new("write.update_commit", async () =>
            {
                // Committed update batch — measures real commit overhead vs rollback.
                await using var tx = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted);
                long updated = 0;
                for (var i = 0; i < cfg.WriteBatch; i++)
                {
                    var id = (commitUpdateCursor + i) % cfg.Rows + 1;
                    var person = await people.SelectAsync(id, tx);
                    if (person == null)
                        continue;

                    person.Score += 1.0;
                    await people.UpdateAsync(person, tx);
                    updated++;
                }

                commitUpdateCursor = (commitUpdateCursor + cfg.WriteBatch) % cfg.Rows;
                await tx.CommitAsync();
                return updated;
            }),
            new("write.delete_rollback", async () =>
            {
                await using var tx = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted);
                long deleted = 0;
                for (var i = 0; i < cfg.WriteBatch; i++)
                {
                    var id = (deleteCursor + i) % cfg.Rows + 1;
                    if (await people.DeleteAsync(id, tx))
                        deleted++;
                }

                deleteCursor = (deleteCursor + cfg.WriteBatch) % cfg.Rows;
                await tx.RollbackAsync();
                return deleted;
            })
        };

        Console.WriteLine("SharpDB benchmark");
        Console.WriteLine(
            $"rows={cfg.Rows} warmup={cfg.Warmup} iterations={cfg.Iterations} " +
            $"select_batch={cfg.SelectBatch} write_batch={cfg.WriteBatch} wal={cfg.EnableWal} db={dbPath}");
        Console.WriteLine();

        Console.WriteLine(
            $"{PadRight("case", 28)}{PadLeft("avg_ms", 10)}{PadLeft("p50", 10)}{PadLeft("p95", 10)}" +
            $"{PadLeft("min", 10)}{PadLeft("max", 10)}{PadLeft("qps", 10)}{PadLeft("rows_out", 12)}" +
            $"{PadLeft("gc0/iter", 10)}{PadLeft("gc1/iter", 10)}{PadLeft("gc2/iter", 10)}");
        Console.WriteLine(new string('-', 130));

        foreach (var benchmarkCase in cases)
        {
            var stats = await RunBenchmarkCaseAsync(benchmarkCase, cfg);
            Console.WriteLine(
                $"{PadRight(benchmarkCase.Name, 28)}" +
                $"{PadLeft(Format(stats.AvgMs, 3), 10)}" +
                $"{PadLeft(Format(stats.P50Ms, 3), 10)}" +
                $"{PadLeft(Format(stats.P95Ms, 3), 10)}" +
                $"{PadLeft(Format(stats.MinMs, 3), 10)}" +
                $"{PadLeft(Format(stats.MaxMs, 3), 10)}" +
                $"{PadLeft(Format(stats.Qps, 1), 10)}" +
                $"{PadLeft(Format(stats.AvgRowsOut, 1), 12)}" +
                $"{PadLeft(Format(stats.AvgGc0, 2), 10)}" +
                $"{PadLeft(Format(stats.AvgGc1, 2), 10)}" +
                $"{PadLeft(Format(stats.AvgGc2, 2), 10)}");
        }
    }
    finally
    {
        if (ownsDbPath && Directory.Exists(dbPath))
            Directory.Delete(dbPath, recursive: true);
    }

    return;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Benchmark error: {ex.Message}");
    Environment.ExitCode = 1;
}
finally
{
    Log.CloseAndFlush();
}

static async Task PopulateBaseDataAsync(CollectionManager<BenchmarkPerson, long> collection, int rows)
{
    for (var i = 1; i <= rows; i++)
        await collection.InsertAsync(BuildPerson(i));
}

static BenchmarkPerson BuildPerson(long id)
{
    return new BenchmarkPerson
    {
        Id = id,
        Name = $"person-{id}",
        Email = $"person{id}@example.com",
        Age = 20 + (int)(id % 50),
        Score = 100 + (id % 500) * 0.1,
        IsActive = id % 2 == 0,
        CreatedDate = DateTime.UtcNow.AddMinutes(-id)
    };
}

static async Task<BenchStats> RunBenchmarkCaseAsync(BenchCase benchCase, BenchConfig cfg)
{
    for (var i = 0; i < cfg.Warmup; i++)
        await benchCase.RunAsync();

    var samplesMs = new List<double>(cfg.Iterations);
    double rowsTotal = 0;
    int totalGc0 = 0, totalGc1 = 0, totalGc2 = 0;

    for (var i = 0; i < cfg.Iterations; i++)
    {
        var gc0Before = GC.CollectionCount(0);
        var gc1Before = GC.CollectionCount(1);
        var gc2Before = GC.CollectionCount(2);

        var sw = Stopwatch.StartNew();
        var rowsOut = await benchCase.RunAsync();
        sw.Stop();

        totalGc0 += GC.CollectionCount(0) - gc0Before;
        totalGc1 += GC.CollectionCount(1) - gc1Before;
        totalGc2 += GC.CollectionCount(2) - gc2Before;

        samplesMs.Add(sw.Elapsed.TotalMilliseconds);
        rowsTotal += rowsOut;
    }

    samplesMs.Sort();
    var avgMs = samplesMs.Average();

    return new BenchStats(
        avgMs,
        Percentile(samplesMs, 0.50),
        Percentile(samplesMs, 0.95),
        samplesMs[0],
        samplesMs[^1],
        avgMs > 0 ? 1000.0 / avgMs : 0.0,
        rowsTotal / cfg.Iterations,
        (double)totalGc0 / cfg.Iterations,
        (double)totalGc1 / cfg.Iterations,
        (double)totalGc2 / cfg.Iterations);
}

static double Percentile(IReadOnlyList<double> sortedSamples, double percentile)
{
    if (sortedSamples.Count == 0)
        return 0;
    if (sortedSamples.Count == 1)
        return sortedSamples[0];

    var position = percentile * (sortedSamples.Count - 1);
    var lo = (int)position;
    var hi = Math.Min(sortedSamples.Count - 1, lo + 1);
    var frac = position - lo;
    return sortedSamples[lo] + (sortedSamples[hi] - sortedSamples[lo]) * frac;
}

static BenchConfig ParseArgs(IReadOnlyList<string> args)
{
    var cfg = new BenchConfig();

    for (var i = 0; i < args.Count; i++)
    {
        var arg = args[i];
        string NextValue()
        {
            if (i + 1 >= args.Count)
                throw new ArgumentException($"Missing value for {arg}");
            i++;
            return args[i];
        }

        switch (arg)
        {
            case "--help":
            case "-h":
                PrintUsage();
                Environment.Exit(0);
                break;
            case "--rows":
                cfg.Rows = int.Parse(NextValue(), CultureInfo.InvariantCulture);
                break;
            case "--warmup":
                cfg.Warmup = int.Parse(NextValue(), CultureInfo.InvariantCulture);
                break;
            case "--iters":
                cfg.Iterations = int.Parse(NextValue(), CultureInfo.InvariantCulture);
                break;
            case "--select-batch":
                cfg.SelectBatch = int.Parse(NextValue(), CultureInfo.InvariantCulture);
                break;
            case "--write-batch":
                cfg.WriteBatch = int.Parse(NextValue(), CultureInfo.InvariantCulture);
                break;
            case "--db":
                cfg.DbPath = NextValue();
                break;
            case "--wal":
                cfg.EnableWal = ParseBool(NextValue());
                break;
            default:
                throw new ArgumentException($"Unknown argument: {arg}");
        }
    }

    if (cfg.Rows <= 0)
        throw new ArgumentException("--rows must be > 0");
    if (cfg.Warmup < 0)
        throw new ArgumentException("--warmup must be >= 0");
    if (cfg.Iterations <= 0)
        throw new ArgumentException("--iters must be > 0");
    if (cfg.SelectBatch <= 0)
        throw new ArgumentException("--select-batch must be > 0");
    if (cfg.WriteBatch <= 0)
        throw new ArgumentException("--write-batch must be > 0");

    return cfg;
}

static bool ParseBool(string value)
{
    if (bool.TryParse(value, out var parsed))
        return parsed;

    return value switch
    {
        "1" => true,
        "0" => false,
        _ => throw new ArgumentException($"Invalid boolean value: {value}")
    };
}

static void PrintUsage()
{
    Console.WriteLine(
        "Usage: dotnet run --project SharpDB.Benchmark/SharpDB.Benchmark.csproj -- [options]\n" +
        "Options:\n" +
        "  --rows N           Base dataset size (default: 20000)\n" +
        "  --warmup N         Warmup rounds per case (default: 2)\n" +
        "  --iters N          Measured rounds per case (default: 10)\n" +
        "  --select-batch N   Lookups/range window size for read cases (default: 1000)\n" +
        "  --write-batch N    Rows per rollback write case (default: 500)\n" +
        "  --wal true|false   Enable WAL during benchmark (default: false)\n" +
        "  --db PATH          Reuse a specific DB path (default: temp directory)\n");
}

static string Format(double value, int decimals)
{
    return value.ToString($"F{decimals}", CultureInfo.InvariantCulture);
}

static string PadLeft(string value, int width) => value.PadLeft(width);
static string PadRight(string value, int width) => value.PadRight(width);

static Schema PersonSchema()
{
    return new Schema
    {
        Fields =
        [
            new Field { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
            new Field { Name = "Name", Type = FieldType.String, MaxLength = 120 },
            new Field { Name = "Email", Type = FieldType.String, MaxLength = 255 },
            new Field { Name = "Age", Type = FieldType.Int },
            new Field { Name = "Score", Type = FieldType.Double },
            new Field { Name = "IsActive", Type = FieldType.Bool },
            new Field { Name = "CreatedDate", Type = FieldType.DateTime }
        ]
    };
}

public sealed class BenchmarkPerson
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public int Age { get; set; }
    public double Score { get; set; }
    public bool IsActive { get; set; }
    public DateTime CreatedDate { get; set; }
}

internal sealed class BenchConfig
{
    public int Rows { get; set; } = 20000;
    public int Warmup { get; set; } = 2;
    public int Iterations { get; set; } = 10;
    public int SelectBatch { get; set; } = 1000;
    public int WriteBatch { get; set; } = 500;
    public bool EnableWal { get; set; }
    public string? DbPath { get; set; }
}

internal readonly record struct BenchCase(string Name, Func<Task<long>> RunAsync);

internal readonly record struct BenchStats(
    double AvgMs,
    double P50Ms,
    double P95Ms,
    double MinMs,
    double MaxMs,
    double Qps,
    double AvgRowsOut,
    double AvgGc0,
    double AvgGc1,
    double AvgGc2);
