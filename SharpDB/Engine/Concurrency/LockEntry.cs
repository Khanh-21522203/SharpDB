using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Engine.Concurrency;

public class LockEntry
{
    private readonly SemaphoreSlim _exclusiveLock = new(1, 1);
    private readonly SemaphoreSlim _sharedLock = new(int.MaxValue, int.MaxValue);
    private int _sharedCount = 0;
    private TransactionId? _exclusiveHolder = null;
    private readonly HashSet<TransactionId> _sharedHolders = new();
    
    public async Task AcquireSharedAsync(TransactionId txnId, CancellationToken ct)
    {
        await _sharedLock.WaitAsync(ct);
        
        if (_exclusiveHolder != null && _exclusiveHolder != txnId)
        {
            _sharedLock.Release();
            await _exclusiveLock.WaitAsync(ct);
            _exclusiveLock.Release();
            await AcquireSharedAsync(txnId, ct);
            return;
        }
        
        _sharedHolders.Add(txnId);
        Interlocked.Increment(ref _sharedCount);
    }
    
    public async Task AcquireExclusiveAsync(TransactionId txnId, CancellationToken ct)
    {
        await _exclusiveLock.WaitAsync(ct);
        
        // Wait for all shared locks to release
        while (_sharedCount > 0 && !_sharedHolders.Contains(txnId))
        {
            _exclusiveLock.Release();
            await Task.Delay(10, ct);
            await _exclusiveLock.WaitAsync(ct);
        }
        
        _exclusiveHolder = txnId;
    }
    
    public void Release(TransactionId txnId)
    {
        if (_exclusiveHolder == txnId)
        {
            _exclusiveHolder = null;
            _exclusiveLock.Release();
        }
        else if (_sharedHolders.Remove(txnId))
        {
            Interlocked.Decrement(ref _sharedCount);
            _sharedLock.Release();
        }
    }
}