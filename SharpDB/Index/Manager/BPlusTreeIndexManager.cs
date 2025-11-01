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
    private readonly DeleteOperation<TK, TV> _deleteOp;
    private readonly int _indexId;
    private readonly InsertOperation<TK, TV> _insertOp;
    private readonly SearchOperation<TK, TV> _searchOp;
    private readonly IIndexIOSession<TK> _session;
    private readonly IIndexStorageManager _storage;

    public BPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree)
    {
        _storage = storage;
        _indexId = indexId;

        var keySerializer = CreateSerializer<TK>();
        var valueSerializer = CreateSerializer<TV>();
        var factory = new BPlusTreeNodeFactory<TK, TV>(keySerializer, valueSerializer, degree);

        _session = new BufferedIndexIOSession<TK>(storage, (INodeFactory<TK, object>)factory, indexId);
        _searchOp = new SearchOperation<TK, TV>(_session, storage, indexId);
        _insertOp = new InsertOperation<TK, TV>(_session, storage, factory, indexId);
        _deleteOp = new DeleteOperation<TK, TV>(_session, storage, indexId);
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
        await foreach (var _ in _searchOp.RangeSearchAsync(default!, default!)) count++;
        return count;
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
        throw new NotSupportedException($"Type {type} not supported");
    }
}