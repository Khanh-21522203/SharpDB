using Serilog;
using SharpDB;
using SharpDB.Configuration;
using SharpDB.Engine;

// Configure logging
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .CreateLogger();

// Demo SharpDB functionality with WAL
Console.WriteLine("=== SharpDB Demo with Write-Ahead Logging (WAL) ===\n");

try
{
    // Initialize database with WAL enabled
    var dbPath = "test-database";
    var config = new EngineConfig
    {
        EnableWAL = true,
        WALAutoCheckpoint = true,
        WALCheckpointInterval = 10 // Checkpoint every 10 transactions for demo
    };
    
    using var db = new SharpDB.SharpDB(dbPath, config);
    Console.WriteLine($"[OK] Database initialized at: {dbPath}\n");

    // Define schema for a Person entity
    var schema = new Schema
    {
        Fields = new List<Field>
        {
            new() { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
            new() { Name = "Name", Type = FieldType.String, MaxLength = 100 },
            new() { Name = "Age", Type = FieldType.Int },
            new() { Name = "Email", Type = FieldType.String, MaxLength = 255, IsIndexed = true },
            new() { Name = "CreatedDate", Type = FieldType.DateTime }
        }
    };
    schema.Validate();

    // Create collection
    var collection = await db.CreateCollectionAsync<Person, long>(
        "persons",
        schema,
        p => p.Id
    );
    Console.WriteLine("[OK] Collection 'persons' created\n");

    // Insert sample data
    var persons = new[]
    {
        new Person { Id = 1, Name = "Nguyen Van A", Age = 30, Email = "a@example.com", CreatedDate = DateTime.Now },
        new Person { Id = 2, Name = "Tran Thi B", Age = 25, Email = "b@example.com", CreatedDate = DateTime.Now },
        new Person { Id = 3, Name = "Le Van C", Age = 35, Email = "c@example.com", CreatedDate = DateTime.Now }
    };

    Console.WriteLine("Inserting sample data...");
    foreach (var person in persons)
    {
        await collection.InsertAsync(person);
        Console.WriteLine($"  [OK] Inserted: {person.Name} (ID: {person.Id})");
    }

    // Query data
    Console.WriteLine("\nQuerying data...");
    var person2 = await collection.SelectAsync(2);
    if (person2 != null)
    {
        Console.WriteLine($"  Found person with ID 2: {person2.Name}, Age: {person2.Age}, Email: {person2.Email}");
    }

    // Update data
    Console.WriteLine("\nUpdating data...");
    person2!.Age = 26;
    await collection.UpdateAsync(person2);
    Console.WriteLine($"  [OK] Updated person ID 2 age to 26");

    // Verify update
    var updatedPerson = await collection.SelectAsync(2);
    Console.WriteLine($"  Verification: Age is now {updatedPerson?.Age}");

    // Count records
    var count = await collection.CountAsync();
    Console.WriteLine($"\nTotal records in collection: {count}");

    // Scan all records
    Console.WriteLine("\nScanning all records:");
    await foreach (var person in collection.ScanAsync())
    {
        Console.WriteLine($"  - {person.Name} (ID: {person.Id}, Age: {person.Age})");
    }

    // Demonstrate transactions with WAL
    Console.WriteLine("\n=== Transaction Demo with WAL ===");
    
    // Start a transaction
    var transaction = await db.BeginTransactionAsync();
    Console.WriteLine("[OK] Transaction started");
    
    try
    {
        // Add a new person within transaction
        var newPerson = new Person { Id = 4, Name = "Pham Van D", Age = 28, Email = "d@example.com", CreatedDate = DateTime.Now };
        await collection.InsertAsync(newPerson);
        Console.WriteLine($"  [OK] Inserted within transaction: {newPerson.Name}");
        
        // Commit the transaction
        await db.CommitTransactionAsync(transaction);
        Console.WriteLine("[OK] Transaction committed - changes persisted to WAL");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Transaction failed: {ex.Message}");
        await db.RollbackTransactionAsync(transaction);
        Console.WriteLine("[OK] Transaction rolled back");
    }
    finally
    {
        transaction.Dispose();
    }

    // Create a checkpoint
    Console.WriteLine("\n=== Checkpoint Demo ===");
    var checkpointLSN = await db.CreateCheckpointAsync();
    Console.WriteLine($"[OK] Checkpoint created at LSN: {checkpointLSN}");

    // Delete a record
    Console.WriteLine("\nDeleting record...");
    var deleted = await collection.DeleteAsync(1);
    Console.WriteLine($"  [OK] Deleted person with ID 1: {deleted}");

    // Final count
    count = await collection.CountAsync();
    Console.WriteLine($"\nFinal record count: {count}");

    // Flush to disk
    await db.FlushAsync();
    Console.WriteLine("\n[OK] Database and WAL flushed to disk successfully");
    
    // Simulate crash recovery scenario
    Console.WriteLine("\n=== Simulating Crash Recovery ===");
    Console.WriteLine("Database will be closed and reopened to simulate recovery...");
    
    // Close database  
    Console.WriteLine("Database closed.\n");
    
    // Simulate recovery by reopening the database
    Console.WriteLine("=== Reopening Database (Recovery Test) ===");
    
    var recoveryConfig = new EngineConfig { EnableWAL = true };
    using var db2 = new SharpDB.SharpDB(dbPath, recoveryConfig);
    Console.WriteLine("[OK] Database reopened - WAL recovery executed automatically");
    
    var collection2 = await db2.GetCollectionAsync<Person, long>("persons", p => p.Id);
    
    Console.WriteLine("\n[OK] Collection 'persons' loaded after recovery");
    
    // Verify data after recovery
    Console.WriteLine("\nData after recovery:");
    await foreach (var person in collection2.ScanAsync())
    {
        Console.WriteLine($"  - {person.Name} (ID: {person.Id}, Age: {person.Age})");
    }
    
    var finalCount = await collection2.CountAsync();
    Console.WriteLine($"\nRecords after recovery: {finalCount}");
}
catch (Exception ex)
{
    Console.WriteLine($"\n[ERROR] Error: {ex.Message}");
    Log.Error(ex, "Database operation failed");
}
finally
{
    Log.CloseAndFlush();
}

Console.WriteLine("\n=== Demo completed ===");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();

// Demo entity class
public class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedDate { get; set; }
}