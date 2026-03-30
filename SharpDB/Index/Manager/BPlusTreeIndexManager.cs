using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.Index.Node;
using SharpDB.Index.Operations;
using SharpDB.Index.Session;
using SharpDB.Serialization;

namespace SharpDB.Index.Manager;

public class BPlusTreeIndexManager<TK, TV> : IUniqueTreeIndexManager<TK, TV>
    where TK : IComparable<TK>
{
    private readonly IBPlusTreeMutationEngine<TK, TV> _mutationEngine;
    private readonly SearchOperation<TK, TV> _searchOp;
    private readonly IIndexIOSession<TK> _session;
    private readonly BPlusTreeNodeFactory<TK, TV> _factory;
    private readonly AsyncReaderWriterLock _gate = new();
    private readonly IIndexStorageManager _storage;
    private readonly int _indexId;

    public BPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree)
    {
        var keySerializer = SerializerRegistry.GetSerializer<TK>();
        var valueSerializer = SerializerRegistry.GetSerializer<TV>();
        _factory = new BPlusTreeNodeFactory<TK, TV>(keySerializer, valueSerializer, degree);

        _storage = storage;
        _indexId = indexId;
        _session = new BufferedIndexIOSession<TK>(storage, CreateObjectNodeFactory(), indexId);
        _searchOp = new SearchOperation<TK, TV>(_session, storage, indexId);
        _mutationEngine = new BPlusTreeMutationEngine<TK, TV>(
            new InsertOperation<TK, TV>(_session, storage, _factory, indexId),
            new DeleteOperation<TK, TV>(_session, storage, indexId),
            _session,
            storage,
            indexId);
    }

    // Constructor with explicit serializers (for custom types like BinaryList)
    public BPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree,
        ISerializer<TK> keySerializer,
        ISerializer<TV> valueSerializer)
    {
        _factory = new BPlusTreeNodeFactory<TK, TV>(keySerializer, valueSerializer, degree);

        _storage = storage;
        _indexId = indexId;
        _session = new BufferedIndexIOSession<TK>(storage, CreateObjectNodeFactory(), indexId);
        _searchOp = new SearchOperation<TK, TV>(_session, storage, indexId);
        _mutationEngine = new BPlusTreeMutationEngine<TK, TV>(
            new InsertOperation<TK, TV>(_session, storage, _factory, indexId),
            new DeleteOperation<TK, TV>(_session, storage, indexId),
            _session,
            storage,
            indexId);
    }
    
    private INodeFactory<TK, object> CreateObjectNodeFactory()
    {
        return new ObjectNodeFactoryAdapter<TK, TV>(_factory, allowCreateInternalNode: true);
    }

    public async Task<TV?> GetAsync(TK key)
    {
        await _gate.EnterReadLockAsync();
        try
        {
            return await _searchOp.SearchAsync(key);
        }
        finally
        {
            _gate.ExitReadLock();
        }
    }

    public async Task PutAsync(TK key, TV value)
    {
        await _gate.EnterWriteLockAsync();
        try
        {
            await _mutationEngine.MutateAsync(new MutationRequest<TK, TV>(MutationKind.Upsert, key, value));
        }
        finally
        {
            _gate.ExitWriteLock();
        }
    }

    public async Task<bool> RemoveAsync(TK key)
    {
        await _gate.EnterWriteLockAsync();
        try
        {
            var result = await _mutationEngine.MutateAsync(new MutationRequest<TK, TV>(MutationKind.Delete, key));
            return result.Applied;
        }
        finally
        {
            _gate.ExitWriteLock();
        }
    }

    public async Task<bool> ContainsKeyAsync(TK key)
    {
        var value = await GetAsync(key);
        return value != null;
    }

    public async Task<int> CountAsync()
    {
        await _gate.EnterReadLockAsync();
        try
        {
            // Traverse leaf nodes via NextLeaf links, summing KeyCount.
            // Avoids deserializing values — much faster than a full range scan.
            return await _searchOp.CountLeafKeysAsync();
        }
        finally
        {
            _gate.ExitReadLock();
        }
    }
    
    public async Task FlushAsync()
    {
        await _gate.EnterWriteLockAsync();
        try
        {
            await _mutationEngine.CommitAsync();
        }
        finally
        {
            _gate.ExitWriteLock();
        }
    }

    public async Task ResetAsync()
    {
        await _gate.EnterWriteLockAsync();
        try
        {
            _session.Clear();
            await _storage.TruncateIndexAsync(_indexId);
        }
        finally
        {
            _gate.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        _session?.Dispose();
        _gate.Dispose();
    }

}
