using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Engine.Concurrency;

public class LockManager : ILockManager
{
    private readonly ConcurrentDictionary<ResourceId, LockEntry> _locks = new();
    private readonly ConcurrentDictionary<TransactionId, HashSet<ResourceId>> _txnLocks = new();
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

    public void Dispose()
    {
        _locks.Clear();
        _txnLocks.Clear();
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