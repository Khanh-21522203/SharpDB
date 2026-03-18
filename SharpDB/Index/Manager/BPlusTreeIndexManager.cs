using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
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
    private readonly SemaphoreSlim _gate = new(1, 1);

    public BPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree)
    {
        var keySerializer = CreateSerializer<TK>();
        var valueSerializer = CreateSerializer<TV>();
        _factory = new BPlusTreeNodeFactory<TK, TV>(keySerializer, valueSerializer, degree);

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
        await _gate.WaitAsync();
        try
        {
            return await _searchOp.SearchAsync(key);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task PutAsync(TK key, TV value)
    {
        await _gate.WaitAsync();
        try
        {
            await _mutationEngine.MutateAsync(new MutationRequest<TK, TV>(MutationKind.Upsert, key, value));
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> RemoveAsync(TK key)
    {
        await _gate.WaitAsync();
        try
        {
            var result = await _mutationEngine.MutateAsync(new MutationRequest<TK, TV>(MutationKind.Delete, key));
            return result.Applied;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> ContainsKeyAsync(TK key)
    {
        var value = await GetAsync(key);
        if (value == null)
            return false;

        if (value is Pointer pointer)
            return pointer.Type == Pointer.TypeData && pointer.Position > 0;

        return true;
    }

    public async Task<int> CountAsync()
    {
        await _gate.WaitAsync();
        try
        {
            // Traverse leaf nodes via NextLeaf links, summing KeyCount.
            // Avoids deserializing values — much faster than a full range scan.
            return await _searchOp.CountLeafKeysAsync();
        }
        finally
        {
            _gate.Release();
        }
    }
    
    private T GetMinValue<T>() where T : IComparable<T>
    {
        var type = typeof(T);
        if (type == typeof(long)) return (T)(object)long.MinValue;
        if (type == typeof(int)) return (T)(object)int.MinValue;
        if (type == typeof(string)) return (T)(object)string.Empty;
        if (type == typeof(DateTime)) return (T)(object)DateTime.MinValue;
        if (type == typeof(decimal)) return (T)(object)decimal.MinValue;
        return default!;
    }
    
    private T GetMaxValue<T>() where T : IComparable<T>
    {
        var type = typeof(T);
        if (type == typeof(long)) return (T)(object)long.MaxValue;
        if (type == typeof(int)) return (T)(object)int.MaxValue;
        if (type == typeof(string)) return (T)(object)new string('\uffff', 255); // Max unicode string
        if (type == typeof(DateTime)) return (T)(object)DateTime.MaxValue;
        if (type == typeof(decimal)) return (T)(object)decimal.MaxValue;
        return default!;
    }

    public async Task FlushAsync()
    {
        await _gate.WaitAsync();
        try
        {
            await _mutationEngine.CommitAsync();
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        _session?.Dispose();
        _gate.Dispose();
    }

    private ISerializer<T> CreateSerializer<T>()
    {
        var type = typeof(T);
        if (type == typeof(long)) return (ISerializer<T>)new LongSerializer();
        if (type == typeof(int)) return (ISerializer<T>)new IntSerializer();
        if (type == typeof(string)) return (ISerializer<T>)new StringSerializer(255);
        if (type == typeof(Pointer)) return (ISerializer<T>)new PointerSerializer();
        throw new NotSupportedException($"Type {type} not supported");
    }
}
