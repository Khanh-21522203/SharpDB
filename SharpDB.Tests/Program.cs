using System.IO.Compression;
using SharpDB;
using SharpDB.Configuration;
using SharpDB.DataStructures;
using SharpDB.Engine;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Engine.Concurrency;
using SharpDB.Engine.Transaction;
using SharpDB.Serialization;
using SharpDB.Storage.Database;
using SharpDB.WAL;

internal static class Program
{
    private static async Task<int> Main()
    {
        var tests = new (string Name, Func<Task> Run)[]
        {
            ("WAL recovery round-trip", TestWalRecoveryRoundTripAsync),
            ("Transaction boundary write/read", TestTransactionBoundaryWriteReadAsync),
            ("Transactional collection serializer consistency", TestTransactionalCollectionSerializerConsistencyAsync),
            ("Transactional insert honors collection id", TestTransactionalInsertHonorsCollectionIdAsync),
            ("Transactional insert rollback restores primary key availability", TestTransactionalInsertRollbackRestoresPrimaryKeyAvailabilityAsync),
            ("Transactional delete rollback restores index visibility", TestTransactionalDeleteRollbackRestoresIndexVisibilityAsync),
            ("MVCC read-your-own-writes", TestMvccReadYourOwnWritesAsync),
            ("MVCC deterministic latest committed version", TestMvccDeterministicLatestCommittedAsync),
            ("MVCC blocks dirty reads", TestMvccBlocksDirtyReadsAsync),
            ("MVCC abort cleanup visibility", TestMvccAbortCleanupVisibilityAsync),
            ("Serializable range lock blocks concurrent writes", TestSerializableRangeLockBlocksConcurrentWritesAsync),
            ("Compressed storage read safety", TestCompressedStorageReadSafetyAsync),
            ("Secondary index query API", TestSecondaryIndexQueryApiAsync),
            ("Foreign key validation", TestForeignKeyValidationAsync),
            ("Collection inner join API", TestCollectionInnerJoinApiAsync),
            ("WAL recovery handles missing-pointer inserts", TestWalRecoveryInsertWhenPointerMissingAsync),
            ("WAL recovery applies delete records", TestWalRecoveryDeleteRecordAsync),
            ("Concurrent collection inserts stress", TestConcurrentCollectionInsertsStressAsync),
            ("WAL reopen loop stress", TestWalReopenLoopStressAsync),
            ("WAL auto-checkpoint interval", TestWalAutoCheckpointIntervalAsync),
            ("WAL auto-checkpoint requires WAL enabled", TestWalAutoCheckpointRequiresWalEnabledAsync),
            ("WAL auto-checkpoint failure does not break commit", TestWalAutoCheckpointFailureDoesNotBreakCommitAsync)
        };

        foreach (var test in tests)
        {
            try
            {
                await test.Run();
                Console.WriteLine($"[PASS] {test.Name}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FAIL] {test.Name}: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                return 1;
            }
        }

        Console.WriteLine("All SharpDB tests passed.");
        return 0;
    }

    private static async Task TestWalRecoveryRoundTripAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-wal-test-{Guid.NewGuid()}");

        try
        {
            var config = new EngineConfig { EnableWAL = true };

            await using (var scope = new AsyncDbScope(basePath, config))
            {
                var collection = await scope.GetOrCreateCollectionAsync();
                await collection.InsertAsync(new Person { Id = 1, Name = "Alice", Age = 30, Email = "alice@example.com", CreatedDate = DateTime.UtcNow });
                await collection.InsertAsync(new Person { Id = 2, Name = "Bob", Age = 26, Email = "bob@example.com", CreatedDate = DateTime.UtcNow });
                await scope.Db.FlushAsync();
            }

            await using (var scope = new AsyncDbScope(basePath, new EngineConfig { EnableWAL = true }))
            {
                var collection = await scope.Db.GetCollectionAsync<Person, long>("persons", p => p.Id);
                var count = await collection.CountAsync();
                Assert(count == 2, $"Expected 2 records after WAL recovery, got {count}");

                var person = await collection.SelectAsync(1);
                Assert(person != null, "Record with ID 1 not found after WAL recovery");
                Assert(person!.Name.Trim() == "Alice", $"Expected Alice after WAL recovery, got {person.Name}");
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestTransactionBoundaryWriteReadAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-tx-test-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var pointer = await db.Transactions.ExecuteAsync(async tx =>
            {
                var person = new Person
                {
                    Id = 42,
                    Name = "Tx Person",
                    Age = 31,
                    Email = "tx@example.com",
                    CreatedDate = DateTime.UtcNow
                };

                var writePointer = await tx.WriteAsync<Person>(null, person);
                var readBack = await tx.ReadAsync<Person>(writePointer);

                Assert(readBack != null, "Transaction read-back returned null");
                Assert(readBack!.Id == 42, $"Expected ID 42, got {readBack.Id}");

                return writePointer;
            });

            var verify = await db.Transactions.ExecuteAsync(async tx => await tx.ReadAsync<Person>(pointer));
            Assert(verify != null, "Committed transaction data not visible in subsequent transaction");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestTransactionalCollectionSerializerConsistencyAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-tx-collection-serializer-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var people = await db.CreateCollectionAsync<Person, long>("persons", PersonSchema(), p => p.Id);

            await db.Transactions.ExecuteAsync(async tx =>
            {
                var person = new Person
                {
                    Id = 700,
                    Name = "TxCollection",
                    Age = 40,
                    Email = "tx-collection@example.com",
                    CreatedDate = DateTime.UtcNow
                };

                await people.InsertAsync(person, tx);

                var inTx = await people.SelectAsync(700, tx);
                Assert(inTx != null && inTx.Name.Trim() == "TxCollection",
                    "Collection transactional read should decode payload with collection serializer");

                return 0;
            }, IsolationLevel.ReadCommitted);

            var persisted = await people.SelectAsync(700);
            Assert(persisted != null && persisted.Name.Trim() == "TxCollection",
                "Committed collection transactional write should remain readable through collection API");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestTransactionalInsertHonorsCollectionIdAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-tx-collection-id-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var c1 = await db.CreateCollectionAsync<Person, long>("c1", PersonSchema(), p => p.Id);
            var c2 = await db.CreateCollectionAsync<Person, long>("c2", PersonSchema(), p => p.Id);

            await db.Transactions.ExecuteAsync(async tx =>
            {
                await c2.InsertAsync(new Person
                {
                    Id = 900,
                    Name = "Collection2",
                    Age = 20,
                    Email = "c2@example.com",
                    CreatedDate = DateTime.UtcNow
                }, tx);
                return 0;
            });

            var c1ScanCount = 0;
            await foreach (var _ in c1.ScanAsync())
                c1ScanCount++;

            var c2ScanCount = 0;
            await foreach (var _ in c2.ScanAsync())
                c2ScanCount++;

            Assert(c1ScanCount == 0, $"Expected collection c1 scan count 0, got {c1ScanCount}");
            Assert(c2ScanCount == 1, $"Expected collection c2 scan count 1, got {c2ScanCount}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestTransactionalInsertRollbackRestoresPrimaryKeyAvailabilityAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-tx-insert-rollback-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var people = await db.CreateCollectionAsync<Person, long>("people", PersonSchema(), p => p.Id);

            await using (var tx = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted))
            {
                await people.InsertAsync(new Person
                {
                    Id = 1001,
                    Name = "TempInsert",
                    Age = 44,
                    Email = "temp-insert@example.com",
                    CreatedDate = DateTime.UtcNow
                }, tx);

                await tx.RollbackAsync();
            }

            var rolledBack = await people.SelectAsync(1001);
            Assert(rolledBack == null, "Rolled-back transactional insert should not be visible");

            await people.InsertAsync(new Person
            {
                Id = 1001,
                Name = "ReusedAfterRollback",
                Age = 45,
                Email = "reuse@example.com",
                CreatedDate = DateTime.UtcNow
            });

            var count = await people.CountAsync();
            Assert(count == 1, $"Expected primary key to be reusable after rollback, count={count}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestTransactionalDeleteRollbackRestoresIndexVisibilityAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-tx-delete-rollback-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var people = await db.CreateCollectionAsync<Person, long>("people", PersonSchema(), p => p.Id);

            await people.InsertAsync(new Person
            {
                Id = 2001,
                Name = "ToDelete",
                Age = 29,
                Email = "delete@example.com",
                CreatedDate = DateTime.UtcNow
            });

            await using (var tx = await db.Transactions.BeginAsync(IsolationLevel.ReadCommitted))
            {
                var deleted = await people.DeleteAsync(2001, tx);
                Assert(deleted, "Transactional delete should report success");

                var inTxRead = await people.SelectAsync(2001, tx);
                Assert(inTxRead == null, "Deleted row should not be visible inside deleting transaction");

                await tx.RollbackAsync();
            }

            var restored = await people.SelectAsync(2001);
            Assert(restored != null && restored.Name.Trim() == "ToDelete",
                "Rollback should restore primary index visibility for deleted row");

            var count = await people.CountAsync();
            Assert(count == 1, $"Expected count=1 after delete rollback, got {count}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestMvccReadYourOwnWritesAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-mvcc-own-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var tx = await db.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            var pointer = await tx.WriteAsync<Person>(null, new Person
            {
                Id = 1,
                Name = "MVCC-Own",
                Age = 10,
                Email = "mvcc-own@example.com",
                CreatedDate = DateTime.UtcNow
            });

            var ownRead = await tx.ReadAsync<Person>(pointer);
            Assert(ownRead != null && ownRead.Name.Trim() == "MVCC-Own", "Transaction should read its own uncommitted write");

            var updatedPointer = await tx.WriteAsync<Person>(pointer, new Person
            {
                Id = 1,
                Name = "MVCC-Own-Updated",
                Age = 11,
                Email = "mvcc-own@example.com",
                CreatedDate = DateTime.UtcNow
            });

            var readViaOldPointer = await tx.ReadAsync<Person>(pointer);
            Assert(readViaOldPointer != null && readViaOldPointer.Name.Trim() == "MVCC-Own-Updated",
                "Read via original pointer should observe own latest uncommitted version");

            var readViaNewPointer = await tx.ReadAsync<Person>(updatedPointer);
            Assert(readViaNewPointer != null && readViaNewPointer.Name.Trim() == "MVCC-Own-Updated",
                "Read via new pointer should observe own latest uncommitted version");

            await db.RollbackTransactionAsync(tx);
            tx.Dispose();
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestMvccDeterministicLatestCommittedAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-mvcc-commit-order-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var tx = await db.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            var p1 = await tx.WriteAsync<Person>(null, new Person
            {
                Id = 11,
                Name = "V1",
                Age = 1,
                Email = "v1@example.com",
                CreatedDate = DateTime.UtcNow
            });

            var p2 = await tx.WriteAsync<Person>(p1, new Person
            {
                Id = 11,
                Name = "V2",
                Age = 2,
                Email = "v2@example.com",
                CreatedDate = DateTime.UtcNow
            });

            await db.CommitTransactionAsync(tx);
            tx.Dispose();

            var viaP1 = await db.Transactions.ExecuteAsync(async session => await session.ReadAsync<Person>(p1));
            var viaP2 = await db.Transactions.ExecuteAsync(async session => await session.ReadAsync<Person>(p2));

            Assert(viaP1 != null && viaP1.Name.Trim() == "V2",
                "Latest committed version was not returned via original pointer");
            Assert(viaP2 != null && viaP2.Name.Trim() == "V2",
                "Latest committed version was not returned via new pointer");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestMvccBlocksDirtyReadsAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-mvcc-dirty-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var writer = await db.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            var pointer = await writer.WriteAsync<Person>(null, new Person
            {
                Id = 2,
                Name = "WriterUncommitted",
                Age = 20,
                Email = "writer@example.com",
                CreatedDate = DateTime.UtcNow
            });

            var reader = await db.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            var dirtyRead = await reader.ReadAsync<Person>(pointer);
            Assert(dirtyRead == null, "Different transaction should not see uncommitted version");

            await db.RollbackTransactionAsync(reader);
            await db.RollbackTransactionAsync(writer);
            reader.Dispose();
            writer.Dispose();
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestMvccAbortCleanupVisibilityAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-mvcc-abort-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var tx = await db.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            var pointer = await tx.WriteAsync<Person>(null, new Person
            {
                Id = 3,
                Name = "AbortMe",
                Age = 30,
                Email = "abort@example.com",
                CreatedDate = DateTime.UtcNow
            });

            await db.RollbackTransactionAsync(tx);
            tx.Dispose();

            var verify = await db.BeginTransactionAsync(IsolationLevel.ReadCommitted);
            var readAfterAbort = await verify.ReadAsync<Person>(pointer);
            Assert(readAfterAbort == null, "Aborted version must not be visible after rollback");

            await db.RollbackTransactionAsync(verify);
            verify.Dispose();
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestSerializableRangeLockBlocksConcurrentWritesAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-serializable-range-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var people = await db.CreateCollectionAsync<Person, long>("people", PersonSchema(), p => p.Id);

            await people.InsertAsync(new Person
            {
                Id = 1,
                Name = "Seed",
                Age = 20,
                Email = "seed@example.com",
                CreatedDate = DateTime.UtcNow
            });

            await using var rangeReader = await db.Transactions.BeginAsync(IsolationLevel.Serializable);
            await foreach (var _ in people.RangeQueryAsync(1, 100, rangeReader))
                break;

            var writerStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var writerFinished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var writerTask = Task.Run(async () =>
            {
                await using var writer = await db.Transactions.BeginAsync(IsolationLevel.Serializable);
                writerStarted.TrySetResult(true);
                await people.InsertAsync(new Person
                {
                    Id = 2,
                    Name = "BlockedUntilReaderEnds",
                    Age = 21,
                    Email = "blocked@example.com",
                    CreatedDate = DateTime.UtcNow
                }, writer);
                await writer.CommitAsync();
                writerFinished.TrySetResult(true);
            });

            await writerStarted.Task;
            await Task.Delay(200);
            Assert(!writerFinished.Task.IsCompleted, "Serializable writer should wait while range read lock is held");

            await rangeReader.CommitAsync();
            await writerTask;
            Assert(writerFinished.Task.IsCompleted, "Serializable writer should proceed after range lock release");

            var count = await people.CountAsync();
            Assert(count == 2, $"Expected writer commit to add one indexed row, got count={count}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestCompressedStorageReadSafetyAsync()
    {
        var config = EngineConfig.Default;
        using var inner = new MemoryDatabaseStorageManager(config);
        using var compressed = new CompressedDatabaseStorageManager(inner, CompressionLevel.Fastest, compressionThreshold: 1);

        var payload = Enumerable.Repeat((byte)'A', 4096).ToArray();
        var pointer = await compressed.StoreAsync(1, 1, 1, payload);

        var obj1 = await compressed.SelectAsync(pointer);
        Assert(obj1 != null, "Compressed SelectAsync returned null on first read");
        Assert(obj1!.Data.SequenceEqual(payload), "First decompressed read does not match original payload");

        var obj2 = await compressed.SelectAsync(pointer);
        Assert(obj2 != null, "Compressed SelectAsync returned null on second read");
        Assert(obj2!.Data.SequenceEqual(payload), "Second decompressed read does not match original payload");
    }

    private static async Task TestSecondaryIndexQueryApiAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-secondary-query-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var employees = await db.CreateCollectionAsync<Employee, long>("employees", EmployeeSchema(), e => e.Id);

            await employees.CreateSecondaryIndexAsync<int>("Age", e => e.Age, isUnique: false);
            await employees.CreateSecondaryIndexAsync<int>("Badge", e => e.Badge, isUnique: true);

            await employees.InsertAsync(new Employee { Id = 1, Name = "Alice", DepartmentId = 10, Age = 30, Badge = 100 });
            await employees.InsertAsync(new Employee { Id = 2, Name = "Bob", DepartmentId = 10, Age = 30, Badge = 200 });
            await employees.InsertAsync(new Employee { Id = 3, Name = "Cara", DepartmentId = 20, Age = 26, Badge = 300 });

            var uniqueHit = await employees.SelectBySecondaryIndexAsync<int>("Badge", 200);
            Assert(uniqueHit != null && uniqueHit.Name.Trim() == "Bob", "Unique secondary index lookup failed");

            var age30 = new List<Employee>();
            await foreach (var employee in employees.SelectManyBySecondaryIndexAsync<int>("Age", 30))
                age30.Add(employee);
            Assert(age30.Count == 2, $"Expected 2 employees with Age=30, got {age30.Count}");

            var badgeRange = new List<Employee>();
            await foreach (var employee in employees.RangeBySecondaryIndexAsync<int>("Badge", 150, 350))
                badgeRange.Add(employee);
            Assert(badgeRange.Count == 2, $"Expected 2 employees in badge range [150,350], got {badgeRange.Count}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestForeignKeyValidationAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-fk-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var departments = await db.CreateCollectionAsync<Department, long>("departments", DepartmentSchema(), d => d.Id);
            await departments.InsertAsync(new Department { Id = 10, Name = "Engineering" });

            var employees = await db.CreateCollectionAsync<Employee, long>("employees", EmployeeSchemaWithForeignKey(), e => e.Id);

            await employees.InsertAsync(new Employee { Id = 1, Name = "Alice", DepartmentId = 10, Age = 30, Badge = 111 });

            var failed = false;
            try
            {
                await employees.InsertAsync(new Employee { Id = 2, Name = "BrokenFK", DepartmentId = 999, Age = 22, Badge = 222 });
            }
            catch (InvalidOperationException)
            {
                failed = true;
            }

            Assert(failed, "Expected foreign key violation for DepartmentId=999");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestCollectionInnerJoinApiAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-join-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });

            var departments = await db.CreateCollectionAsync<Department, long>("departments", DepartmentSchema(), d => d.Id);
            var employees = await db.CreateCollectionAsync<Employee, long>("employees", EmployeeSchema(), e => e.Id);

            await departments.InsertAsync(new Department { Id = 10, Name = "Engineering" });
            await departments.InsertAsync(new Department { Id = 20, Name = "Finance" });

            await employees.InsertAsync(new Employee { Id = 1, Name = "Alice", DepartmentId = 10, Age = 30, Badge = 1001 });
            await employees.InsertAsync(new Employee { Id = 2, Name = "Bob", DepartmentId = 20, Age = 25, Badge = 1002 });
            await employees.InsertAsync(new Employee { Id = 3, Name = "NoMatch", DepartmentId = 30, Age = 40, Badge = 1003 });

            var hashJoinRows = new List<(Employee Left, Department Right)>();
            await foreach (var row in employees.InnerJoinAsync<Department, long, long>(
                               departments,
                               e => e.DepartmentId,
                               d => d.Id))
                hashJoinRows.Add(row);

            Assert(hashJoinRows.Count == 2, $"Expected 2 hash-join rows, got {hashJoinRows.Count}");

            var pkJoinRows = new List<(Employee Left, Department Right)>();
            await foreach (var row in employees.InnerJoinByRightPrimaryKeyAsync(departments, e => e.DepartmentId))
                pkJoinRows.Add(row);

            Assert(pkJoinRows.Count == 2, $"Expected 2 primary-key join rows, got {pkJoinRows.Count}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestWalRecoveryInsertWhenPointerMissingAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-wal-insert-recover-{Guid.NewGuid()}");

        try
        {
            var config = new EngineConfig { EnableWAL = true };
            using var storage = new MemoryDatabaseStorageManager(config);
            using var wal = new WALManager(basePath, storage, config.WALMaxFileSize);

            var payload = new BinaryObjectSerializer(PersonSchema()).Serialize(new Person
            {
                Id = 501,
                Name = "RecoverInsert",
                Age = 22,
                Email = "recover-insert@example.com",
                CreatedDate = DateTime.UtcNow
            });

            const long txnId = 99;
            wal.LogTransactionBegin(txnId);
            wal.LogInsert(txnId, 77, new Pointer(Pointer.TypeData, 12345, 77), payload);
            wal.LogTransactionCommit(txnId);
            wal.Flush();

            await wal.RecoverAsync();

            var count = 0;
            await foreach (var _ in storage.ScanAsync(77))
                count++;

            Assert(count >= 1, "Expected REDO insert to materialize at least one row in target collection");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestWalRecoveryDeleteRecordAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-wal-delete-recover-{Guid.NewGuid()}");

        try
        {
            var config = new EngineConfig { EnableWAL = true };
            using var storage = new MemoryDatabaseStorageManager(config);
            using var wal = new WALManager(basePath, storage, config.WALMaxFileSize);

            var payload = new BinaryObjectSerializer(PersonSchema()).Serialize(new Person
            {
                Id = 601,
                Name = "RecoverDelete",
                Age = 33,
                Email = "recover-delete@example.com",
                CreatedDate = DateTime.UtcNow
            });

            var pointer = await storage.StoreAsync(1, 88, 1, payload);

            const long txnId = 100;
            wal.LogTransactionBegin(txnId);
            wal.LogDelete(txnId, 88, pointer, payload);
            wal.LogTransactionCommit(txnId);
            wal.Flush();

            await wal.RecoverAsync();

            var selected = await storage.SelectAsync(pointer);
            Assert(selected == null, "Expected REDO delete to remove record");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestConcurrentCollectionInsertsStressAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-concurrent-insert-{Guid.NewGuid()}");

        try
        {
            using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
            var people = await db.CreateCollectionAsync<Person, long>("stress_people", PersonSchema(), p => p.Id);

            const int total = 40;
            var tasks = Enumerable.Range(1, total).Select(i =>
                Task.Run(async () =>
                {
                    await people.InsertAsync(new Person
                    {
                        Id = i,
                        Name = $"P{i}",
                        Age = 20 + (i % 10),
                        Email = $"p{i}@example.com",
                        CreatedDate = DateTime.UtcNow
                    });
                })).ToArray();

            await Task.WhenAll(tasks);

            var count = await people.CountAsync();
            Assert(count == total, $"Expected {total} records after concurrent inserts, got {count}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestWalReopenLoopStressAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-reopen-loop-{Guid.NewGuid()}");

        try
        {
            const int cycles = 3;

            for (var cycle = 1; cycle <= cycles; cycle++)
            {
                using var db = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true });
                CollectionManager<Person, long> people;

                if (cycle == 1)
                    people = await db.CreateCollectionAsync<Person, long>("loop_people", PersonSchema(), p => p.Id);
                else
                    people = await db.GetCollectionAsync<Person, long>("loop_people", p => p.Id);

                await people.InsertAsync(new Person
                {
                    Id = cycle,
                    Name = $"Cycle{cycle}",
                    Age = 30 + cycle,
                    Email = $"cycle{cycle}@example.com",
                    CreatedDate = DateTime.UtcNow
                });

                await db.FlushAsync();
            }

            using (var verifyDb = new SharpDB.SharpDB(basePath, new EngineConfig { EnableWAL = true }))
            {
                var people = await verifyDb.GetCollectionAsync<Person, long>("loop_people", p => p.Id);
                var count = await people.CountAsync();
                Assert(count == cycles, $"Expected {cycles} rows after reopen loop, got {count}");
            }
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestWalAutoCheckpointIntervalAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-autocp-interval-{Guid.NewGuid()}");

        try
        {
            var config = new EngineConfig
            {
                EnableWAL = true,
                WALAutoCheckpoint = true,
                WALCheckpointInterval = 2
            };

            using var db = new SharpDB.SharpDB(basePath, config);

            for (var i = 1; i <= 5; i++)
            {
                await db.Transactions.ExecuteAsync(async tx =>
                {
                    var person = new Person
                    {
                        Id = i,
                        Name = $"AutoCP-{i}",
                        Age = 20 + i,
                        Email = $"auto-{i}@example.com",
                        CreatedDate = DateTime.UtcNow
                    };

                    return await tx.WriteAsync<Person>(null, person);
                });
            }

            await WaitForConditionAsync(
                () => CountWalRecords(basePath, LogRecordType.CheckpointStart) >= 2,
                TimeSpan.FromSeconds(5),
                "Expected at least 2 checkpoint start records for 5 commits with interval 2");

            var checkpointStarts = CountWalRecords(basePath, LogRecordType.CheckpointStart);
            var checkpointEnds = CountWalRecords(basePath, LogRecordType.CheckpointEnd);
            Assert(checkpointStarts == 2, $"Expected exactly 2 checkpoint starts, got {checkpointStarts}");
            Assert(checkpointEnds == 2, $"Expected exactly 2 checkpoint ends, got {checkpointEnds}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestWalAutoCheckpointRequiresWalEnabledAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-autocp-nowal-{Guid.NewGuid()}");

        try
        {
            var config = new EngineConfig
            {
                EnableWAL = false,
                WALAutoCheckpoint = true,
                WALCheckpointInterval = 1
            };

            using var db = new SharpDB.SharpDB(basePath, config);

            for (var i = 1; i <= 3; i++)
            {
                await db.Transactions.ExecuteAsync(async tx =>
                {
                    var person = new Person
                    {
                        Id = i,
                        Name = $"NoWAL-{i}",
                        Age = 30 + i,
                        Email = $"nowal-{i}@example.com",
                        CreatedDate = DateTime.UtcNow
                    };

                    return await tx.WriteAsync<Person>(null, person);
                });
            }

            await Task.Delay(200);
            var walPath = Path.Combine(basePath, "wal");
            Assert(!Directory.Exists(walPath), "WAL directory should not exist when WAL is disabled");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static async Task TestWalAutoCheckpointFailureDoesNotBreakCommitAsync()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"sharpdb-autocp-failure-{Guid.NewGuid()}");

        try
        {
            var config = new EngineConfig { EnableWAL = true };
            using var storage = new MemoryDatabaseStorageManager(config);
            using var walManager = new WALManager(basePath, storage, config.WALMaxFileSize);
            using var lockManager = new LockManager();
            using var versionManager = new VersionManager(storage);
            var serializer = new JsonObjectSerializer();

            var checkpointAttempted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            using var transactionManager = new TransactionManager(
                lockManager,
                versionManager,
                serializer,
                walManager,
                walEnabled: true,
                walAutoCheckpoint: true,
                walCheckpointInterval: 1,
                checkpointFactory: () =>
                {
                    checkpointAttempted.TrySetResult(true);
                    return Task.FromException<long>(new InvalidOperationException("checkpoint failure"));
                });

            var tx = await transactionManager.BeginTransactionAsync();
            await transactionManager.CommitAsync(tx);

            var completionTask = await Task.WhenAny(checkpointAttempted.Task, Task.Delay(TimeSpan.FromSeconds(2)));
            Assert(completionTask == checkpointAttempted.Task, "Expected auto-checkpoint callback to be invoked");

            var commitRecords = CountWalRecords(basePath, LogRecordType.Commit);
            Assert(commitRecords == 1, $"Expected commit to succeed and persist 1 commit WAL record, got {commitRecords}");
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    private static void Assert(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }

    private static async Task WaitForConditionAsync(Func<bool> condition, TimeSpan timeout, string failureMessage)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;

            await Task.Delay(25);
        }

        Assert(condition(), failureMessage);
    }

    private static int CountWalRecords(string basePath, LogRecordType recordType)
    {
        var walPath = Path.Combine(basePath, "wal");
        if (!Directory.Exists(walPath))
            return 0;

        var count = 0;
        var logFiles = Directory.GetFiles(walPath, "wal_*.log")
            .OrderBy(file => file, StringComparer.Ordinal)
            .ToArray();

        foreach (var logFile in logFiles)
        {
            using var stream = new FileStream(logFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new BinaryReader(stream);

            while (stream.Position + sizeof(int) <= stream.Length)
            {
                var size = reader.ReadInt32();
                if (size <= 0 || stream.Position + size > stream.Length)
                    break;

                var payload = reader.ReadBytes(size);
                if (payload.Length != size || payload.Length == 0)
                    break;

                if (payload[0] == (byte)recordType)
                    count++;
            }
        }

        return count;
    }

    private sealed class AsyncDbScope : IAsyncDisposable
    {
        public SharpDB.SharpDB Db { get; }

        public AsyncDbScope(string basePath, EngineConfig config)
        {
            Db = new SharpDB.SharpDB(basePath, config);
        }

        public async Task<CollectionManager<Person, long>> GetOrCreateCollectionAsync()
        {
            var schema = PersonSchema();

            return await Db.CreateCollectionAsync<Person, long>("persons", schema, p => p.Id);
        }

        public ValueTask DisposeAsync()
        {
            Db.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class Person
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
        public DateTime CreatedDate { get; set; }
    }

    private static Schema PersonSchema()
    {
        return new Schema
        {
            Fields =
            [
                new Field { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
                new Field { Name = "Name", Type = FieldType.String, MaxLength = 100 },
                new Field { Name = "Age", Type = FieldType.Int },
                new Field { Name = "Email", Type = FieldType.String, MaxLength = 255 },
                new Field { Name = "CreatedDate", Type = FieldType.DateTime }
            ]
        };
    }

    private sealed class Department
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class Employee
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public long DepartmentId { get; set; }
        public int Age { get; set; }
        public int Badge { get; set; }
    }

    private static Schema DepartmentSchema()
    {
        return new Schema
        {
            Fields =
            [
                new Field { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
                new Field { Name = "Name", Type = FieldType.String, MaxLength = 120 }
            ]
        };
    }

    private static Schema EmployeeSchema()
    {
        return new Schema
        {
            Fields =
            [
                new Field { Name = "Id", Type = FieldType.Long, IsPrimaryKey = true },
                new Field { Name = "Name", Type = FieldType.String, MaxLength = 120 },
                new Field { Name = "DepartmentId", Type = FieldType.Long },
                new Field { Name = "Age", Type = FieldType.Int },
                new Field { Name = "Badge", Type = FieldType.Int }
            ]
        };
    }

    private static Schema EmployeeSchemaWithForeignKey()
    {
        var schema = EmployeeSchema();
        schema.ForeignKeys.Add(new ForeignKeyConstraint
        {
            FieldName = "DepartmentId",
            ReferencedCollection = "departments",
            ReferencedField = "Id",
            AllowNull = false
        });
        return schema;
    }
}
