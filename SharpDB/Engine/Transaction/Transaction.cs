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
    
    // Track locks for RepeatableRead - need to hold them until commit
    private readonly HashSet<ResourceId> _heldReadLocks = new();
    private readonly Stack<Func<Task>> _rollbackActions = new();
    private readonly Lock _rollbackSync = new();
    private long _nextInsertLockId;

    public async Task<byte[]?> ReadBytesAsync(Pointer pointer)
    {
        var resourceId = new ResourceId("record", pointer.ToString());
        var txnId = new TransactionId(TransactionId);
        
        // Handle locking based on isolation level
        switch (IsolationLevel)
        {
            case IsolationLevel.ReadUncommitted:
                // No locks for dirty reads
                break;
                
            case IsolationLevel.ReadCommitted:
                // Acquire lock but release immediately after reading
                if (!await lockManager.AcquireLockAsync(resourceId, txnId, LockMode.Shared, TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Timed out acquiring shared lock for read");
                break;
                
            case IsolationLevel.RepeatableRead:
                // Acquire lock and hold until commit
                if (!await lockManager.AcquireLockAsync(resourceId, txnId, LockMode.Shared, TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Timed out acquiring shared lock for repeatable read");
                _heldReadLocks.Add(resourceId);
                break;
                
            case IsolationLevel.Serializable:
                // For now, same as RepeatableRead but with range locks (TODO)
                if (!await lockManager.AcquireLockAsync(resourceId, txnId, LockMode.Shared, TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Timed out acquiring shared lock for serializable read");
                _heldReadLocks.Add(resourceId);
                break;
        }

        try
        {
            // Read version
            var version = await versionManager.ReadAsync(pointer, StartTimestamp, TransactionId);
            if (version == null)
                return null;

            return version.Data;
        }
        finally
        {
            // Release lock immediately for ReadCommitted
            if (IsolationLevel == IsolationLevel.ReadCommitted)
            {
                await lockManager.ReleaseLockAsync(resourceId, txnId);
            }
        }
    }

    public async Task<T?> ReadAsync<T>(Pointer pointer) where T : class
    {
        var payload = await ReadBytesAsync(pointer);
        if (payload == null)
            return null;

        return serializer.Deserialize<T>(payload);
    }

    public async Task<Pointer> WriteBytesAsync(Pointer? pointer, byte[] data, int? collectionIdHint = null)
    {
        var resourceId = pointer.HasValue && !pointer.Value.IsEmpty()
            ? new ResourceId("record", pointer.Value.ToString())
            : new ResourceId("record", $"new:{TransactionId}:{Interlocked.Increment(ref _nextInsertLockId)}");
        var txnId = new TransactionId(TransactionId);

        // Acquire exclusive lock
        if (!await lockManager.AcquireLockAsync(resourceId, txnId, LockMode.Exclusive, TimeSpan.FromSeconds(10)))
            throw new TimeoutException("Timed out acquiring exclusive lock for write");

        // Get before image for WAL (if updating existing record)
        byte[] beforeImage = Array.Empty<byte>();
        if (pointer != null && !pointer.Value.IsEmpty())
        {
            var oldVersion = await versionManager.ReadAsync(pointer.Value, StartTimestamp, TransactionId);
            if (oldVersion != null)
            {
                beforeImage = oldVersion.Data;
            }
        }

        // Create new version
        var newPointer = await versionManager.WriteAsync(
            pointer,
            data,
            StartTimestamp,
            TransactionId,
            collectionIdHint);

        // Log to WAL if available
        if (walManager != null)
        {
            var targetPointer = pointer ?? newPointer;
            var collectionId = targetPointer.Chunk;
            if (pointer == null || pointer.Value.IsEmpty())
                walManager.LogInsert(TransactionId, collectionId, targetPointer, data);
            else if (data.Length == 0)
                walManager.LogDelete(TransactionId, collectionId, targetPointer, beforeImage);
            else
                walManager.LogUpdate(TransactionId, collectionId, targetPointer, beforeImage, data);
        }

        return newPointer;
    }

    public async Task<Pointer> WriteAsync<T>(Pointer? pointer, T data) where T : class
    {
        var payload = serializer.Serialize(data);
        return await WriteBytesAsync(pointer, payload);
    }

    public async Task AcquireRangeReadLockAsync(string resourceType, string startInclusive, string endInclusive)
    {
        if (IsolationLevel != IsolationLevel.Serializable)
            return;

        var txnId = new TransactionId(TransactionId);
        var start = new ResourceId(resourceType, startInclusive);
        var end = new ResourceId(resourceType, endInclusive);

        if (!await lockManager.AcquireRangeLockAsync(start, end, txnId, LockMode.Shared, TimeSpan.FromSeconds(10)))
            throw new TimeoutException(
                $"Timed out acquiring serializable range lock on {resourceType} [{startInclusive}, {endInclusive}]");
    }

    public async Task AcquireRangeWriteLockAsync(string resourceType, string startInclusive, string endInclusive)
    {
        if (IsolationLevel != IsolationLevel.Serializable)
            return;

        var txnId = new TransactionId(TransactionId);
        var start = new ResourceId(resourceType, startInclusive);
        var end = new ResourceId(resourceType, endInclusive);

        if (!await lockManager.AcquireRangeLockAsync(start, end, txnId, LockMode.Exclusive, TimeSpan.FromSeconds(10)))
            throw new TimeoutException(
                $"Timed out acquiring serializable range write lock on {resourceType} [{startInclusive}, {endInclusive}]");
    }

    public Task RegisterRollbackActionAsync(Func<Task> rollbackAction)
    {
        ArgumentNullException.ThrowIfNull(rollbackAction);

        lock (_rollbackSync)
        {
            _rollbackActions.Push(rollbackAction);
        }

        return Task.CompletedTask;
    }

    public async Task RunRollbackActionsAsync()
    {
        while (true)
        {
            Func<Task>? rollbackAction;

            lock (_rollbackSync)
            {
                rollbackAction = _rollbackActions.Count > 0 ? _rollbackActions.Pop() : null;
            }

            if (rollbackAction == null)
                break;

            await rollbackAction();
        }
    }

    public void Dispose()
    {
        // Locks released on commit/rollback
    }
}
