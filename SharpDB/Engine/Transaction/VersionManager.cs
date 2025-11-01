using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Engine.Transaction;

public class VersionManager(IDatabaseStorageManager storage) : IVersionManager
{
    private readonly ConcurrentDictionary<Pointer, List<VersionedRecord>> _versionChains = new();
    private readonly ConcurrentDictionary<long, HashSet<Pointer>> _txnWrites = new();

    public async Task<VersionedRecord?> ReadAsync(Pointer pointer, long readTimestamp)
    {
        if (!_versionChains.TryGetValue(pointer, out var chain))
            return null;
        
        // Find visible version for read timestamp
        foreach (var version in chain.OrderByDescending(v => v.BeginTimestamp))
        {
            if (version.IsCommitted && 
                version.BeginTimestamp <= readTimestamp && 
                version.EndTimestamp > readTimestamp)
            {
                return version;
            }
        }
        
        return null;
    }
    
    public async Task<Pointer> WriteAsync(Pointer? pointer, byte[] data, long writeTimestamp, long txnId)
    {
        var newVersion = new VersionedRecord
        {
            Data = data,
            BeginTimestamp = writeTimestamp,
            TransactionId = txnId,
            IsCommitted = false,
            PreviousVersion = pointer
        };
        
        // Store new version
        var newPointer = await storage.StoreAsync(1, 1, 1, data);
        newVersion.Pointer = newPointer;
        
        if (pointer != null)
        {
            _versionChains.GetOrAdd(pointer.Value, _ => new List<VersionedRecord>()).Add(newVersion);
        }
        else
        {
            _versionChains[newPointer] = [newVersion];
        }
        
        _txnWrites.GetOrAdd(txnId, _ => []).Add(newPointer);
        
        return newPointer;
    }
    
    public Task CommitAsync(long txnId, long commitTimestamp)
    {
        if (_txnWrites.TryRemove(txnId, out var pointers))
        {
            foreach (var pointer in pointers)
            {
                if (_versionChains.TryGetValue(pointer, out var chain))
                {
                    var version = chain.FirstOrDefault(v => v.TransactionId == txnId);
                    if (version != null)
                    {
                        version.IsCommitted = true;
                        version.BeginTimestamp = commitTimestamp;
                    }
                }
            }
        }
        
        return Task.CompletedTask;
    }
    
    public Task AbortAsync(long txnId)
    {
        if (_txnWrites.TryRemove(txnId, out var pointers))
        {
            foreach (var pointer in pointers)
            {
                if (_versionChains.TryGetValue(pointer, out var chain))
                {
                    chain.RemoveAll(v => v.TransactionId == txnId);
                }
            }
        }
        
        return Task.CompletedTask;
    }
    
    public Task GarbageCollectAsync(long minActiveTimestamp)
    {
        foreach (var (pointer, chain) in _versionChains)
        {
            chain.RemoveAll(v => v.IsCommitted && v.EndTimestamp < minActiveTimestamp);
        }
        
        return Task.CompletedTask;
    }
    
    public void Dispose()
    {
        _versionChains.Clear();
        _txnWrites.Clear();
    }
}