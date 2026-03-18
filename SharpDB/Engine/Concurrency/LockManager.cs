using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Engine.Concurrency;

public class LockManager : ILockManager
{
    private readonly ConcurrentDictionary<ResourceId, LockEntry> _locks = new();
    private readonly ConcurrentDictionary<TransactionId, HashSet<ResourceId>> _txnLocks = new();
    private readonly ConcurrentDictionary<string, RangeLockState> _rangeLocks = new();
    private readonly ConcurrentDictionary<TransactionId, HashSet<string>> _txnRangeLocks = new();
    private readonly DeadlockDetector _deadlockDetector = new();
    private readonly Lock _deadlockLock = new();
    private readonly Lock _rangeSync = new();

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

        if (_txnRangeLocks.TryRemove(txnId, out var rangeKeys))
        {
            foreach (var rangeKey in rangeKeys.ToList())
            {
                if (_rangeLocks.TryGetValue(rangeKey, out var state) && state.Owner == txnId)
                    await ReleaseRangeLockAsync(state.StartId, state.EndId, txnId);
            }
        }
    }

    public Task<bool> IsLockedAsync(ResourceId resourceId)
    {
        return Task.FromResult(_locks.ContainsKey(resourceId));
    }

    public async Task<bool> AcquireRangeLockAsync(ResourceId startId, ResourceId endId, TransactionId txnId, LockMode mode, TimeSpan timeout)
    {
        var rangeKey = GetRangeKey(startId, endId);

        var startLocked = await AcquireLockAsync(startId, txnId, mode, timeout);
        if (!startLocked)
            return false;

        var endLocked = await AcquireLockAsync(endId, txnId, mode, timeout);
        if (!endLocked)
        {
            await ReleaseLockAsync(startId, txnId);
            return false;
        }

        lock (_rangeSync)
        {
            _rangeLocks[rangeKey] = new RangeLockState(startId, endId, txnId);
            var byTxn = _txnRangeLocks.GetOrAdd(txnId, _ => new HashSet<string>());
            byTxn.Add(rangeKey);
        }

        return true;
    }
    
    public async Task ReleaseRangeLockAsync(ResourceId startId, ResourceId endId, TransactionId txnId)
    {
        var rangeKey = GetRangeKey(startId, endId);

        await ReleaseLockAsync(startId, txnId);
        await ReleaseLockAsync(endId, txnId);

        lock (_rangeSync)
        {
            if (_rangeLocks.TryGetValue(rangeKey, out var state) && state.Owner == txnId)
                _rangeLocks.TryRemove(rangeKey, out _);

            if (_txnRangeLocks.TryGetValue(txnId, out var keys))
            {
                keys.Remove(rangeKey);
                if (keys.Count == 0)
                    _txnRangeLocks.TryRemove(txnId, out _);
            }
        }
    }
    
    public void Dispose()
    {
        _locks.Clear();
        _txnLocks.Clear();
        _rangeLocks.Clear();
        _txnRangeLocks.Clear();
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

    private static string GetRangeKey(ResourceId startId, ResourceId endId)
    {
        return $"{startId.Type}:{startId.Id}-{endId.Id}";
    }

    private sealed record RangeLockState(ResourceId StartId, ResourceId EndId, TransactionId Owner);
}
