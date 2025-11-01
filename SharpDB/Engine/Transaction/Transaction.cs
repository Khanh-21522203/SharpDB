using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Engine.Transaction;

public class Transaction(
    long txnId,
    long startTimestamp,
    IsolationLevel level,
    ILockManager lockManager,
    IVersionManager versionManager,
    IObjectSerializer serializer)
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
        
        // Create new version
        var bytes = serializer.Serialize(data);
        return await versionManager.WriteAsync(pointer, bytes, StartTimestamp, TransactionId);
    }
    
    public void Dispose()
    {
        // Locks released on commit/rollback
    }
}