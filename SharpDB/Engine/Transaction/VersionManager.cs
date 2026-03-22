using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Engine.Transaction;

public class VersionManager(IDatabaseStorageManager storage) : IVersionManager
{
    private const int LockBuckets = 256;
    private readonly object[] _stripedLocks = Enumerable.Range(0, LockBuckets).Select(_ => new object()).ToArray();
    private readonly ConcurrentDictionary<Pointer, List<VersionedRecord>> _versionChains = new();
    private readonly ConcurrentDictionary<long, List<PendingVersionEntry>> _pendingByTxn = new();
    private readonly ConcurrentDictionary<Pointer, Pointer> _chainKeyByPointer = new();
    private long _nextWriteOrder;
    private long _nextCommitOrder;

    private object GetLock(Pointer chainKey) => _stripedLocks[(int)((uint)chainKey.GetHashCode() % LockBuckets)];

    public async Task<VersionedRecord?> ReadAsync(Pointer pointer, long readTimestamp, long? transactionId = null)
    {
        var chainKey = ResolveChainKey(pointer);
        if (_versionChains.TryGetValue(chainKey, out var chain))
        {
            lock (GetLock(chainKey))
            {
                if (transactionId.HasValue)
                {
                    // Multiple writes in the same transaction can share the same timestamp.
                    // Prefer the latest appended uncommitted version in the chain (scan backwards).
                    var txnId = transactionId.Value;
                    for (var i = chain.Count - 1; i >= 0; i--)
                    {
                        var v = chain[i];
                        if (!v.IsCommitted && v.TransactionId == txnId)
                            return v.IsDeleted ? null : v;
                    }
                }

                // Find visible committed version: highest BeginTimestamp, then CommitOrder, then WriteOrder.
                VersionedRecord? visible = null;
                foreach (var v in chain)
                {
                    if (!v.IsCommitted || v.BeginTimestamp > readTimestamp || v.EndTimestamp <= readTimestamp)
                        continue;

                    if (visible == null ||
                        v.BeginTimestamp > visible.BeginTimestamp ||
                        (v.BeginTimestamp == visible.BeginTimestamp && v.CommitOrder > visible.CommitOrder) ||
                        (v.BeginTimestamp == visible.BeginTimestamp && v.CommitOrder == visible.CommitOrder && v.WriteOrder > visible.WriteOrder))
                    {
                        visible = v;
                    }
                }

                if (visible != null)
                    return visible.IsDeleted ? null : visible;
            }

            // A tracked chain exists but nothing is visible for this transaction snapshot.
            // Do not fallback to raw storage here to avoid exposing uncommitted versions.
            return null;
        }

        // Fallback to persisted storage when no chain has ever been tracked for this pointer.
        var dbObject = await storage.SelectAsync(pointer);
        if (dbObject == null)
            return null;

        return new VersionedRecord
        {
            Pointer = pointer,
            Data = dbObject.Data,
            BeginTimestamp = 0,
            EndTimestamp = long.MaxValue,
            IsCommitted = true
        };
    }

    public async Task<Pointer> WriteAsync(
        Pointer? pointer,
        byte[] data,
        long writeTimestamp,
        long txnId,
        int? collectionIdHint = null)
    {
        var isDeleteMarker = data.Length == 0 && pointer.HasValue && !pointer.Value.IsEmpty();

        var collectionId = pointer.HasValue && pointer.Value.Chunk > 0
            ? pointer.Value.Chunk
            : collectionIdHint ?? 1;

        var newPointer = isDeleteMarker
            ? pointer!.Value
            : await storage.StoreAsync(1, collectionId, 1, data);
        var chainKey = pointer.HasValue && !pointer.Value.IsEmpty() ? ResolveChainKey(pointer.Value) : newPointer;

        var newVersion = new VersionedRecord
        {
            Pointer = newPointer,
            Data = data,
            BeginTimestamp = writeTimestamp,
            EndTimestamp = long.MaxValue,
            TransactionId = txnId,
            PreviousVersion = pointer,
            IsCommitted = false,
            WriteOrder = Interlocked.Increment(ref _nextWriteOrder),
            IsDeleted = isDeleteMarker
        };

        lock (GetLock(chainKey))
        {
            var chain = _versionChains.GetOrAdd(chainKey, _ => []);
            chain.Add(newVersion);
            _chainKeyByPointer[chainKey] = chainKey;
            _chainKeyByPointer[newPointer] = chainKey;

            var pending = _pendingByTxn.GetOrAdd(txnId, _ => []);
            pending.Add(new PendingVersionEntry(chainKey, newVersion));
        }

        return newPointer;
    }

    public Task CommitAsync(long txnId, long commitTimestamp)
    {
        if (!_pendingByTxn.TryRemove(txnId, out var pendingVersions))
            return Task.CompletedTask;

        // Acquire all relevant chain locks in a consistent (sorted) order to prevent deadlocks
        // while atomically committing all versions of the transaction. This ensures no reader
        // can observe a partially-committed transaction.
        var buckets = pendingVersions
            .Select(p => (int)((uint)p.ChainKey.GetHashCode() % LockBuckets))
            .Distinct()
            .Order()
            .ToArray();

        AcquireAll(buckets);
        try
        {
            foreach (var pending in pendingVersions)
            {
                if (!_versionChains.TryGetValue(pending.ChainKey, out var chain))
                    continue;

                VersionedRecord? previousCommitted = null;
                foreach (var v in chain)
                {
                    if (!v.IsCommitted || ReferenceEquals(v, pending.Version) || v.EndTimestamp != long.MaxValue)
                        continue;

                    if (previousCommitted == null ||
                        v.BeginTimestamp > previousCommitted.BeginTimestamp ||
                        (v.BeginTimestamp == previousCommitted.BeginTimestamp && v.CommitOrder > previousCommitted.CommitOrder) ||
                        (v.BeginTimestamp == previousCommitted.BeginTimestamp && v.CommitOrder == previousCommitted.CommitOrder && v.WriteOrder > previousCommitted.WriteOrder))
                    {
                        previousCommitted = v;
                    }
                }

                if (previousCommitted != null)
                    previousCommitted.EndTimestamp = commitTimestamp;

                pending.Version.IsCommitted = true;
                pending.Version.BeginTimestamp = commitTimestamp;
                pending.Version.CommitOrder = Interlocked.Increment(ref _nextCommitOrder);
            }
        }
        finally
        {
            ReleaseAll(buckets);
        }

        return Task.CompletedTask;
    }

    private void AcquireAll(int[] sortedBuckets)
    {
        foreach (var b in sortedBuckets)
            Monitor.Enter(_stripedLocks[b]);
    }

    private void ReleaseAll(int[] sortedBuckets)
    {
        foreach (var b in sortedBuckets)
            Monitor.Exit(_stripedLocks[b]);
    }

    public async Task AbortAsync(long txnId)
    {
        if (!_pendingByTxn.TryRemove(txnId, out var pendingVersions))
            return;

        var pointersToDelete = new List<Pointer>();

        foreach (var pending in pendingVersions)
        {
            lock (GetLock(pending.ChainKey))
            {
                if (!pending.Version.IsDeleted)
                {
                    pointersToDelete.Add(pending.Version.Pointer);
                    _chainKeyByPointer.TryRemove(pending.Version.Pointer, out _);
                }

                if (_versionChains.TryGetValue(pending.ChainKey, out var chain))
                {
                    chain.RemoveAll(v => ReferenceEquals(v, pending.Version));
                    if (chain.Count == 0)
                    {
                        _versionChains.TryRemove(pending.ChainKey, out _);
                        _chainKeyByPointer.TryRemove(pending.ChainKey, out _);
                    }
                }
            }
        }

        // Best effort cleanup of physically written but aborted versions.
        foreach (var pointer in pointersToDelete)
        {
            try
            {
                await storage.DeleteAsync(pointer);
            }
            catch
            {
                // Ignore cleanup errors here; logical MVCC state already removed.
            }
        }
    }

    public Task GarbageCollectAsync(long minActiveTimestamp)
    {
        foreach (var key in _versionChains.Keys.ToList())
        {
            lock (GetLock(key))
            {
                if (!_versionChains.TryGetValue(key, out var chain))
                    continue;

                chain.RemoveAll(v => v.IsCommitted && v.EndTimestamp < minActiveTimestamp);

                if (chain.Count == 0)
                {
                    _versionChains.TryRemove(key, out _);
                    _chainKeyByPointer.TryRemove(key, out _);
                }
            }
        }

        return Task.CompletedTask;
    }

    public Task FlushStorageAsync()
    {
        return storage.FlushAsync();
    }

    public void Dispose()
    {
        _versionChains.Clear();
        _pendingByTxn.Clear();
        _chainKeyByPointer.Clear();
    }

    private sealed record PendingVersionEntry(Pointer ChainKey, VersionedRecord Version);

    private Pointer ResolveChainKey(Pointer pointer)
    {
        if (_chainKeyByPointer.TryGetValue(pointer, out var chainKey))
            return chainKey;

        return pointer;
    }
}
