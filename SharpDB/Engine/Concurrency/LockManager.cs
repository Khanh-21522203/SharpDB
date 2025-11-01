using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Engine.Concurrency;

public class LockManager : ILockManager
{
    private readonly ConcurrentDictionary<ResourceId, LockEntry> _locks = new();
    private readonly ConcurrentDictionary<TransactionId, HashSet<ResourceId>> _txnLocks = new();
    
    public async Task<bool> AcquireLockAsync(
        ResourceId resourceId, 
        TransactionId txnId, 
        LockMode mode, 
        TimeSpan timeout)
    {
        var entry = _locks.GetOrAdd(resourceId, _ => new LockEntry());
        
        using var cts = new CancellationTokenSource(timeout);
        
        try
        {
            if (mode == LockMode.Shared)
                await entry.AcquireSharedAsync(txnId, cts.Token);
            else
                await entry.AcquireExclusiveAsync(txnId, cts.Token);
            
            _txnLocks.GetOrAdd(txnId, _ => new HashSet<ResourceId>()).Add(resourceId);
            return true;
        }
        catch (OperationCanceledException)
        {
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
        {
            foreach (var resourceId in resources)
            {
                await ReleaseLockAsync(resourceId, txnId);
            }
        }
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
}
