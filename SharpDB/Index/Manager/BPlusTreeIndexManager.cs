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
    private readonly DeleteOperation<TK, TV> _deleteOp;
    private readonly int _indexId;
    private readonly InsertOperation<TK, TV> _insertOp;
    private readonly SearchOperation<TK, TV> _searchOp;
    private readonly IIndexIOSession<TK> _session;
    private readonly IIndexStorageManager _storage;
    private readonly BPlusTreeNodeFactory<TK, TV> _factory;

    public BPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree)
    {
        _storage = storage;
        _indexId = indexId;

        var keySerializer = CreateSerializer<TK>();
        var valueSerializer = CreateSerializer<TV>();
        _factory = new BPlusTreeNodeFactory<TK, TV>(keySerializer, valueSerializer, degree);

        _session = new BufferedIndexIOSession<TK>(storage, CreateObjectNodeFactory(), indexId);
        _searchOp = new SearchOperation<TK, TV>(_session, storage, indexId);
        _insertOp = new InsertOperation<TK, TV>(_session, storage, _factory, indexId);
        _deleteOp = new DeleteOperation<TK, TV>(_session, storage, indexId);
    }

    // Constructor with explicit serializers (for custom types like BinaryList)
    public BPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree,
        ISerializer<TK> keySerializer,
        ISerializer<TV> valueSerializer)
    {
        _storage = storage;
        _indexId = indexId;

        _factory = new BPlusTreeNodeFactory<TK, TV>(keySerializer, valueSerializer, degree);

        _session = new BufferedIndexIOSession<TK>(storage, CreateObjectNodeFactory(), indexId);
        _searchOp = new SearchOperation<TK, TV>(_session, storage, indexId);
        _insertOp = new InsertOperation<TK, TV>(_session, storage, _factory, indexId);
        _deleteOp = new DeleteOperation<TK, TV>(_session, storage, indexId);
    }
    
    private INodeFactory<TK, object> CreateObjectNodeFactory()
    {
        return new ObjectNodeFactoryAdapter<TK, TV>(_factory, allowCreateInternalNode: true);
    }

    public async Task<TV?> GetAsync(TK key)
    {
        return await _searchOp.SearchAsync(key);
    }

    public async Task PutAsync(TK key, TV value)
    {
        await _insertOp.InsertAsync(key, value);
    }

    public async Task<bool> RemoveAsync(TK key)
    {
        return await _deleteOp.DeleteAsync(key);
    }

    public async Task<bool> ContainsKeyAsync(TK key)
    {
        var value = await GetAsync(key);
        return value != null;
    }

    public async Task<int> CountAsync()
    {
        var count = 0;
        // Scan entire tree from minimum to maximum possible values
        var minKey = GetMinValue<TK>();
        var maxKey = GetMaxValue<TK>();
        await foreach (var _ in _searchOp.RangeSearchAsync(minKey, maxKey)) count++;
        return count;
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
        await _session.FlushAsync();
    }

    public void Dispose()
    {
        _session?.Dispose();
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