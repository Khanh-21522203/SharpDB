using System.Diagnostics;
using SharpDB.Configuration;
using SharpDB.Engine;
using Serilog;

// Configure logging
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Warning()  // Only warnings and errors
    .WriteTo.Console()
    .CreateLogger();

Console.WriteLine("╔═══════════════════════════════════════════════════════════════╗");
Console.WriteLine("║          SharpDB Performance Benchmark Suite                  ║");
Console.WriteLine("╚═══════════════════════════════════════════════════════════════╝");
Console.WriteLine();

// Cleanup old benchmark databases
var dbPath = $"benchmark-db-{DateTime.Now.Ticks}";
Console.WriteLine($"[OK] Using database path: {dbPath}\n");

// Initialize database with optimized config
var config = new EngineConfig
{
    PageSize = 8192,  // Increase page size for larger nodes
    BTreeDegree = 10,  // Use very small degree for testing
    EnableWAL = false,  // Disable WAL for simpler benchmarking
    Cache = new CacheConfig { PageCacheSize = 2000 }
};

using var db = new SharpDB.SharpDB(dbPath, config);
Console.WriteLine("[OK] Database initialized\n");

// Define schema
var schema = new Schema
{
    Fields = new List<Field>
    {
        new() { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
        new() { Name = "Name", Type = FieldType.String, IsIndexed = false },
        new() { Name = "Email", Type = FieldType.String, IsIndexed = false },  // Disable secondary index for now
        new() { Name = "Age", Type = FieldType.Int, IsIndexed = false },
        new() { Name = "Score", Type = FieldType.Double, IsIndexed = false },
        new() { Name = "IsActive", Type = FieldType.Bool, IsIndexed = false },
        new() { Name = "CreatedDate", Type = FieldType.DateTime, IsIndexed = false }
    }
};

var collection = await db.CreateCollectionAsync<Person, long>(
    "persons",
    schema,
    p => p.Id
);
Console.WriteLine("[OK] Collection 'persons' created with indexes\n");

// Benchmark parameters
var recordCount = 1000;  // Start with 1000 records

Console.WriteLine(new string('═', 65));
Console.WriteLine($"  Testing with {recordCount:N0} records");
Console.WriteLine(new string('═', 65));
Console.WriteLine();

    // 1. INSERT BENCHMARK
    Console.WriteLine($"[1/6] Insert Benchmark ({recordCount:N0} records)");
    
    var sw = Stopwatch.StartNew();
    
    for (int i = 0; i < recordCount; i++)
    {
        try
        {
            var person = new Person
            {
                Id = i,
                Name = $"Person {i}",
                Email = $"person{i}@example.com",
                Age = 20 + (i % 50),
                Score = 75.5 + (i % 25),
                IsActive = i % 2 == 0,
                CreatedDate = DateTime.UtcNow.AddDays(-i)
            };
            
            await collection.InsertAsync(person);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error inserting record {i}: {ex.Message}");
            throw;
        }
    }
    
    sw.Stop();
    var insertTime = sw.Elapsed.TotalMilliseconds;
    var insertThroughput = recordCount / sw.Elapsed.TotalSeconds;
    Console.WriteLine($"  Time: {insertTime:F2} ms");
    Console.WriteLine($"  Throughput: {insertThroughput:F2} ops/sec");
    Console.WriteLine($"  Avg per operation: {insertTime / recordCount:F3} ms\n");
    
    // Flush all data to disk before selects
    await db.FlushAsync();

    // 2. SELECT BY PRIMARY KEY BENCHMARK
    Console.WriteLine($"[2/6] Select by Primary Key Benchmark ({recordCount:N0} queries)");
    sw.Restart();
    
    // Try selecting just the first few records to debug
    for (int i = 0; i < Math.Min(10, recordCount); i++)
    {
        try
        {
            var person = await collection.SelectAsync(i);
            if (person != null)
            {
                Console.WriteLine($"  [OK] Selected record {i}: {person.Name}");
            }
            else
            {
                Console.WriteLine($"  [WARN] Record {i} not found");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  [ERROR] Error selecting record {i}: {ex.Message}");
            throw;
        }
    }
    
    // Now run the full benchmark
    for (int i = 0; i < recordCount; i++)
    {
        var person = await collection.SelectAsync(i);
    }
    
    sw.Stop();
    var selectTime = sw.Elapsed.TotalMilliseconds;
    var selectThroughput = recordCount / sw.Elapsed.TotalSeconds;
    Console.WriteLine($"  Time: {selectTime:F2} ms");
    Console.WriteLine($"  Throughput: {selectThroughput:F2} ops/sec");
    Console.WriteLine($"  Avg per operation: {selectTime / recordCount:F3} ms\n");

    // 3. UPDATE BENCHMARK
    Console.WriteLine($"[3/6] Update Benchmark ({recordCount:N0} updates)");
    sw.Restart();
    
    for (int i = 0; i < recordCount; i++)
    {
        var person = await collection.SelectAsync(i);
        if (person != null)
        {
            person.Age += 1;
            person.Score += 0.5;
            await collection.UpdateAsync(person);
        }
    }
    
    sw.Stop();
    var updateTime = sw.Elapsed.TotalMilliseconds;
    var updateThroughput = recordCount / sw.Elapsed.TotalSeconds;
    Console.WriteLine($"  Time: {updateTime:F2} ms");
    Console.WriteLine($"  Throughput: {updateThroughput:F2} ops/sec");
    Console.WriteLine($"  Avg per operation: {updateTime / recordCount:F3} ms\n");

    // 4. SCAN ALL BENCHMARK
    Console.WriteLine($"[4/6] Scan All Records Benchmark");
    sw.Restart();
    
    var scanCount = 0;
    await foreach (var person in collection.ScanAsync())
    {
        scanCount++;
    }
    
    sw.Stop();
    var scanTime = sw.Elapsed.TotalMilliseconds;
    var scanThroughput = scanCount / sw.Elapsed.TotalSeconds;
    Console.WriteLine($"  Time: {scanTime:F2} ms");
    Console.WriteLine($"  Throughput: {scanThroughput:F2} ops/sec");
    Console.WriteLine($"  Scanned: {scanCount:N0} records\n");

    // 5. COUNT BENCHMARK
    Console.WriteLine($"[5/6] Count Benchmark");
    sw.Restart();
    
    var count = await collection.CountAsync();
    
    sw.Stop();
    Console.WriteLine($"  Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    Console.WriteLine($"  Count: {count:N0} records\n");

    // 6. DELETE BENCHMARK (delete last 10%)
    var deleteCount = recordCount / 10;
    Console.WriteLine($"[6/6] Delete Benchmark ({deleteCount:N0} deletes)");
    sw.Restart();
    
    double deleteThroughput = 0;
    try
    {
        for (int i = recordCount - deleteCount; i < recordCount; i++)
        {
            await collection.DeleteAsync(i);
        }
        
        sw.Stop();
        var deleteTime = sw.Elapsed.TotalMilliseconds;
        deleteThroughput = deleteCount / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"  Time: {deleteTime:F2} ms");
        Console.WriteLine($"  Throughput: {deleteThroughput:F2} ops/sec");
        Console.WriteLine($"  Avg per operation: {deleteTime / deleteCount:F3} ms\n");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  [ERROR] Error during delete: {ex.Message}");
        Console.WriteLine($"  Stack trace: {ex.StackTrace?.Split('\n').FirstOrDefault()}\n");
        deleteThroughput = 0;
    }

// SUMMARY
Console.WriteLine("═══ SUMMARY ═══");
Console.WriteLine($"  Insert:      {insertThroughput:F2} ops/sec");
Console.WriteLine($"  Select:      {selectThroughput:F2} ops/sec");
Console.WriteLine($"  Update:      {updateThroughput:F2} ops/sec");
Console.WriteLine($"  Scan:        {scanThroughput:F2} ops/sec");
Console.WriteLine($"  Delete:      {(deleteThroughput > 0 ? $"{deleteThroughput:F2} ops/sec" : "Failed")}");
Console.WriteLine();

Console.WriteLine("╔═══════════════════════════════════════════════════════════════╗");
Console.WriteLine("║                 Benchmark Completed!                          ║");
Console.WriteLine("╚═══════════════════════════════════════════════════════════════╝");

Log.CloseAndFlush();

public class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public int Age { get; set; }
    public double Score { get; set; }
    public bool IsActive { get; set; }
    public DateTime CreatedDate { get; set; }
}
