using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;
using SharpDB.WAL;

namespace SharpDB.Engine.Transaction;

public class Transaction(
    long txnId,
    long startTimestamp,
    IsolationLevel level,
    ILockManager lockManager,
    IVersionManager versionManager,
    IObjectSerializer serializer,
    WALManager? walManager = null)
    : ITransaction
{
    public long TransactionId { get; } = txnId;
    public long StartTimestamp { get; } = startTimestamp;
    public IsolationLevel IsolationLevel { get; } = level;

    public async Task<T?> ReadAsync<T>(Pointer pointer) where T : class
    {
        // Acquire shared lock
        var resourceId = new ResourceId("record", pointer.ToString());
        var txnId = new TransactionId(TransactionId);

        await lockManager.AcquireLockAsync(resourceId, txnId, LockMode.Shared, TimeSpan.FromSeconds(10));

        // Read version
        var version = await versionManager.ReadAsync(pointer, StartTimestamp);
        if (version == null)
            return null;

        return serializer.Deserialize<T>(version.Data);
    }

    public async Task<Pointer> WriteAsync<T>(Pointer? pointer, T data) where T : class
    {
        var resourceId = new ResourceId("record", pointer?.ToString() ?? "new");
        var txnId = new TransactionId(TransactionId);

        // Acquire exclusive lock
        await lockManager.AcquireLockAsync(resourceId, txnId, LockMode.Exclusive, TimeSpan.FromSeconds(10));

        // Get before image for WAL (if updating existing record)
        byte[] beforeImage = Array.Empty<byte>();
        if (pointer != null && !pointer.Value.IsEmpty())
        {
            var oldVersion = await versionManager.ReadAsync(pointer.Value, StartTimestamp);
            if (oldVersion != null)
            {
                beforeImage = oldVersion.Data;
            }
        }

        // Create new version
        var afterImage = serializer.Serialize(data);
        var newPointer = await versionManager.WriteAsync(pointer, afterImage, StartTimestamp, TransactionId);

        // Log to WAL if available
        if (walManager != null && pointer != null)
        {
            // For now, use collection ID 0 - in real implementation, this would be passed in
            walManager.LogUpdate(TransactionId, 0, pointer.Value, beforeImage, afterImage);
        }

        return newPointer;
    }

    public void Dispose()
    {
        // Locks released on commit/rollback
    }
}