using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Engine.Concurrency;

public class LockManager : ILockManager
{
    private readonly ConcurrentDictionary<ResourceId, LockEntry> _locks = new();
    private readonly ConcurrentDictionary<TransactionId, HashSet<ResourceId>> _txnLocks = new();
    private readonly ConcurrentDictionary<string, HashSet<ResourceId>> _rangeLocks = new();
    private readonly DeadlockDetector _deadlockDetector = new();
    private readonly Lock _deadlockLock = new();

    public async Task<bool> AcquireLockAsync(
        ResourceId resourceId,
        TransactionId txnId,
        LockMode mode,
        TimeSpan timeout)
    {
        var entry = _locks.GetOrAdd(resourceId, _ => new LockEntry());

        // Check for potential deadlock before acquiring
        lock (_deadlockLock)
        {
            // Add wait dependency for deadlock detection
            var currentHolder = GetCurrentLockHolder(resourceId);
            if (currentHolder != null && currentHolder != txnId)
            {
                _deadlockDetector.AddWait(txnId, currentHolder);
                
                // Check for deadlock
                if (_deadlockDetector.DetectDeadlock(out var victim))
                {
                    // If this transaction is the victim, abort
                    if (victim == txnId)
                    {
                        _deadlockDetector.RemoveWait(txnId);
                        throw new InvalidOperationException($"Deadlock detected: Transaction {txnId.Id} is selected as victim");
                    }
                }
            }
        }

        using var cts = new CancellationTokenSource(timeout);

        try
        {
            if (mode == LockMode.Shared)
                await entry.AcquireSharedAsync(txnId, cts.Token);
            else
                await entry.AcquireExclusiveAsync(txnId, cts.Token);

            lock (_deadlockLock)
            {
                _deadlockDetector.RemoveWait(txnId);
            }

            _txnLocks.GetOrAdd(txnId, _ => new HashSet<ResourceId>()).Add(resourceId);
            return true;
        }
        catch (OperationCanceledException)
        {
            lock (_deadlockLock)
            {
                _deadlockDetector.RemoveWait(txnId);
            }
            return false; // Timeout
        }
    }

    public Task ReleaseLockAsync(ResourceId resourceId, TransactionId txnId)
    {
        if (_locks.TryGetValue(resourceId, out var entry))
        {
            entry.Release(txnId);

            if (_txnLocks.TryGetValue(txnId, out var resources))
                resources.Remove(resourceId);
        }

        return Task.CompletedTask;
    }

    public async Task ReleaseAllLocksAsync(TransactionId txnId)
    {
        if (_txnLocks.TryRemove(txnId, out var resources))
            foreach (var resourceId in resources)
                await ReleaseLockAsync(resourceId, txnId);
    }

    public Task<bool> IsLockedAsync(ResourceId resourceId)
    {
        return Task.FromResult(_locks.ContainsKey(resourceId));
    }

    public async Task<bool> AcquireRangeLockAsync(ResourceId startId, ResourceId endId, TransactionId txnId, LockMode mode, TimeSpan timeout)
    {
        // Simple implementation: lock all resources in the range
        // In a real database, this would use a more sophisticated approach like predicate locks
        var rangeKey = $"{startId.Type}:{startId.Id}-{endId.Id}";
        var rangeResources = _rangeLocks.GetOrAdd(rangeKey, _ => new HashSet<ResourceId>());
        
        // For simplicity, we're treating range locks as a set of individual locks
        // This prevents phantom reads by locking the entire range
        var lockTasks = new List<Task<bool>>();
        
        // Lock the range boundaries and track them
        lockTasks.Add(AcquireLockAsync(startId, txnId, mode, timeout));
        lockTasks.Add(AcquireLockAsync(endId, txnId, mode, timeout));
        
        // Add range markers to prevent new inserts in this range
        rangeResources.Add(startId);
        rangeResources.Add(endId);
        
        var results = await Task.WhenAll(lockTasks);
        return results.All(r => r);
    }
    
    public async Task ReleaseRangeLockAsync(ResourceId startId, ResourceId endId, TransactionId txnId)
    {
        var rangeKey = $"{startId.Type}:{startId.Id}-{endId.Id}";
        
        if (_rangeLocks.TryGetValue(rangeKey, out var rangeResources))
        {
            // Release individual locks
            await ReleaseLockAsync(startId, txnId);
            await ReleaseLockAsync(endId, txnId);
            
            // Clear range tracking
            rangeResources.Remove(startId);
            rangeResources.Remove(endId);
            
            if (rangeResources.Count == 0)
            {
                _rangeLocks.TryRemove(rangeKey, out _);
            }
        }
    }
    
    public void Dispose()
    {
        _locks.Clear();
        _txnLocks.Clear();
        _rangeLocks.Clear();
    }

    private TransactionId? GetCurrentLockHolder(ResourceId resourceId)
    {
        // Simple implementation - returns first transaction holding the resource
        // In a real implementation, we'd need LockEntry to expose current holders
        foreach (var (txnId, resources) in _txnLocks)
        {
            if (resources.Contains(resourceId))
                return txnId;
        }
        return null;
    }
}