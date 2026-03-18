using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Engine.Transaction;

public class VersionManager(IDatabaseStorageManager storage) : IVersionManager
{
    private readonly object _sync = new();
    private readonly ConcurrentDictionary<Pointer, List<VersionedRecord>> _versionChains = new();
    private readonly ConcurrentDictionary<long, List<PendingVersionEntry>> _pendingByTxn = new();
    private readonly ConcurrentDictionary<Pointer, Pointer> _chainKeyByPointer = new();
    private long _nextWriteOrder;
    private long _nextCommitOrder;

    public async Task<VersionedRecord?> ReadAsync(Pointer pointer, long readTimestamp, long? transactionId = null)
    {
        var chainKey = ResolveChainKey(pointer);
        if (_versionChains.TryGetValue(chainKey, out var chain))
        {
            lock (_sync)
            {
                if (transactionId.HasValue)
                {
                    // Multiple writes in the same transaction can share the same timestamp.
                    // Prefer the latest appended uncommitted version in the chain.
                    var ownUncommitted = chain
                        .LastOrDefault(v => !v.IsCommitted && v.TransactionId == transactionId.Value);

                    if (ownUncommitted != null)
                        return ownUncommitted.IsDeleted ? null : ownUncommitted;
                }

                var visible = chain
                    .Where(v => v.IsCommitted && v.BeginTimestamp <= readTimestamp && v.EndTimestamp > readTimestamp)
                    .OrderByDescending(v => v.BeginTimestamp)
                    .ThenByDescending(v => v.CommitOrder)
                    .ThenByDescending(v => v.WriteOrder)
                    .FirstOrDefault();

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

        lock (_sync)
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

        lock (_sync)
        {
            foreach (var pending in pendingVersions)
            {
                if (!_versionChains.TryGetValue(pending.ChainKey, out var chain))
                    continue;

                var previousCommitted = chain
                    .Where(v => v.IsCommitted && !ReferenceEquals(v, pending.Version) && v.EndTimestamp == long.MaxValue)
                    .OrderByDescending(v => v.BeginTimestamp)
                    .ThenByDescending(v => v.CommitOrder)
                    .ThenByDescending(v => v.WriteOrder)
                    .FirstOrDefault();

                if (previousCommitted != null)
                    previousCommitted.EndTimestamp = commitTimestamp;

                pending.Version.IsCommitted = true;
                pending.Version.BeginTimestamp = commitTimestamp;
                pending.Version.CommitOrder = Interlocked.Increment(ref _nextCommitOrder);
            }
        }

        return Task.CompletedTask;
    }

    public async Task AbortAsync(long txnId)
    {
        if (!_pendingByTxn.TryRemove(txnId, out var pendingVersions))
            return;

        var pointersToDelete = new List<Pointer>();

        lock (_sync)
        {
            foreach (var pending in pendingVersions)
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
        lock (_sync)
        {
            foreach (var (_, chain) in _versionChains)
                chain.RemoveAll(v => v.IsCommitted && v.EndTimestamp < minActiveTimestamp);
        }

        return Task.CompletedTask;
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
