using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Manager;
using SharpDB.Operations;
using SharpDB.Serialization;

namespace SharpDB.Engine;

/// <summary>
///     Manages a collection of records with schema and indexes.
///     Coordinates CRUD operations, primary index, and secondary indexes.
/// </summary>
public class CollectionManager<T, TKey>(
    int collectionId,
    Schema schema,
    IDataIOSession dataSession,
    IIndexStorageManager indexStorage,
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    IIndexIOSession<TKey> indexSession,
    Func<T, TKey> keyExtractor)
    where T : class
    where TKey : IComparable<TKey>
{
    private readonly Dictionary<string, object> _secondaryIndexes = new();
    private readonly IObjectSerializer _serializer = new BinaryObjectSerializer(schema);
    
    // Wrap primary index with queryable decorator for range queries
    private readonly IUniqueQueryableIndex<TKey, Pointer> _queryableIndex = 
        new UniqueQueryableIndexDecorator<TKey, Pointer>(
            primaryIndex, 
            indexSession, 
            indexStorage, 
            collectionId);

    public async Task InsertAsync(T record)
    {
        var op = new CollectionInsertOperation<T, TKey>(
            dataSession,
            _serializer,
            primaryIndex,
            collectionId,
            record,
            keyExtractor);

        await op.ExecuteAsync();

        // Update secondary indexes (only if there are any)
        if (_secondaryIndexes.Count > 0)
            await UpdateSecondaryIndexesAsync(record, true);
    }

    public async Task<T?> SelectAsync(TKey primaryKey)
    {
        var op = new CollectionSelectOperation<T, TKey>(
            dataSession,
            _serializer,
            primaryIndex,
            primaryKey);

        await op.ExecuteAsync();
        return op.Result;
    }

    public async Task UpdateAsync(T record)
    {
        var op = new CollectionUpdateOperation<T, TKey>(
            dataSession,
            _serializer,
            primaryIndex,
            record,
            keyExtractor);

        await op.ExecuteAsync();

        if (_secondaryIndexes.Count > 0)
            await UpdateSecondaryIndexesAsync(record, false);
    }

    public async Task<bool> DeleteAsync(TKey primaryKey)
    {
        // For secondary index cleanup, we need the full record
        // First, retrieve it before deletion
        T? recordToDelete = null;
        if (_secondaryIndexes.Count > 0)
        {
            recordToDelete = await SelectAsync(primaryKey);
        }

        var op = new CollectionDeleteOperation<TKey>(
            dataSession,
            primaryIndex,
            primaryKey);

        await op.ExecuteAsync();

        // Update secondary indexes if deletion was successful
        if (op.WasDeleted && recordToDelete != null && _secondaryIndexes.Count > 0)
        {
            await UpdateSecondaryIndexesAsync(recordToDelete, false);  // isInsert=false for delete
        }

        return op.WasDeleted;
    }

    public async IAsyncEnumerable<T> ScanAsync()
    {
        await foreach (var dbObject in dataSession.ScanAsync(collectionId))
        {
            // Skip deleted objects (soft delete)
            if (!dbObject.IsAlive)
                continue;

            var record = _serializer.Deserialize<T>(dbObject.Data);
            var key = keyExtractor(record);
            
            // Verify record still exists in primary index
            // (deletes remove from index but may not clean data pages immediately)
            var pointer = await primaryIndex.GetAsync(key);
            if (pointer.Position > 0)
            {
                yield return record;
            }
        }
    }

    public async Task<int> CountAsync()
    {
        return await primaryIndex.CountAsync();
    }

    /// <summary>
    /// Query records within a key range [minKey, maxKey] (inclusive).
    /// </summary>
    public async IAsyncEnumerable<T> RangeQueryAsync(TKey minKey, TKey maxKey)
    {
        await foreach (var kv in _queryableIndex.RangeAsync(minKey, maxKey))
        {
            var dbObject = await dataSession.SelectAsync(kv.Value);
            if (dbObject != null)
            {
                var record = _serializer.Deserialize<T>(dbObject.Data);
                yield return record;
            }
        }
    }

    /// <summary>
    /// Query records with keys greater than the specified key.
    /// </summary>
    public async IAsyncEnumerable<T> GreaterThanAsync(TKey key)
    {
        await foreach (var kv in _queryableIndex.GreaterThanAsync(key))
        {
            var dbObject = await dataSession.SelectAsync(kv.Value);
            if (dbObject != null)
            {
                var record = _serializer.Deserialize<T>(dbObject.Data);
                yield return record;
            }
        }
    }

    /// <summary>
    /// Query records with keys less than the specified key.
    /// </summary>
    public async IAsyncEnumerable<T> LessThanAsync(TKey key)
    {
        await foreach (var kv in _queryableIndex.LessThanAsync(key))
        {
            var dbObject = await dataSession.SelectAsync(kv.Value);
            if (dbObject != null)
            {
                var record = _serializer.Deserialize<T>(dbObject.Data);
                yield return record;
            }
        }
    }

    public async Task FlushAsync()
    {
        await dataSession.FlushAsync();
        await primaryIndex.FlushAsync();
    }

    public async Task CreateSecondaryIndexAsync<TIndexKey>(
        string fieldName,
        Func<T, TIndexKey> indexKeyExtractor,
        bool isUnique = false)
        where TIndexKey : IComparable<TIndexKey>
    {
        var field = schema.GetField(fieldName);
        if (field == null)
            throw new InvalidOperationException($"Field {fieldName} not found");

        if (_secondaryIndexes.ContainsKey(fieldName))
            throw new InvalidOperationException($"Index on {fieldName} already exists");

        // Create appropriate index type
        if (isUnique)
        {
            var index = new BPlusTreeIndexManager<TIndexKey, Pointer>(
                indexStorage,
                collectionId * 1000 + _secondaryIndexes.Count + 1,
                128);

            _secondaryIndexes[fieldName] = new SecondaryIndexWrapper<TIndexKey>(
                index, indexKeyExtractor, true);
        }
        else
        {
            var index = new DuplicateBPlusTreeIndexManager<TIndexKey, Pointer>(
                indexStorage,
                collectionId * 1000 + _secondaryIndexes.Count + 1,
                128,
                CreateSerializer<TIndexKey>(),
                new PointerSerializer());

            _secondaryIndexes[fieldName] = new SecondaryIndexWrapper<TIndexKey>(
                index, indexKeyExtractor, false);
        }
    }

    private async Task UpdateSecondaryIndexesAsync(T record, bool isInsert)
    {
        var primaryKey = keyExtractor(record);
        
        if (isInsert)
        {
            // For insert: get pointer from primary index
            var pointer = await primaryIndex.GetAsync(primaryKey);

            // Check if pointer is valid (not null and has actual data)
            if (pointer is not { Position: > 0 })
                return;

            foreach (var (fieldName, indexWrapper) in _secondaryIndexes)
                // Update secondary index: indexKey → Pointer
                await ((dynamic)indexWrapper).UpdateAsync(record, pointer, isInsert: true);
        }
        else
        {
            // For delete: remove from secondary indexes
            foreach (var (fieldName, indexWrapper) in _secondaryIndexes)
                await ((dynamic)indexWrapper).DeleteAsync(record);
        }
    }

    private ISerializer<TType> CreateSerializer<TType>() where TType : IComparable<TType>
    {
        var type = typeof(TType);
        if (type == typeof(long)) return (ISerializer<TType>)new LongSerializer();
        if (type == typeof(int)) return (ISerializer<TType>)new IntSerializer();
        if (type == typeof(string)) return (ISerializer<TType>)new StringSerializer(255);
        if (type == typeof(DateTime)) return (ISerializer<TType>)new DateTimeSerializer();
        if (type == typeof(decimal)) return (ISerializer<TType>)new DecimalSerializer();
        if (type == typeof(Guid)) return (ISerializer<TType>)(object)new GuidSerializer();
        throw new NotSupportedException($"Type {type} not supported");
    }

    // Helper class to wrap secondary indexes
    private class SecondaryIndexWrapper<TIndexKey>(
        object index,
        Func<T, TIndexKey> indexKeyExtractor,
        bool isUnique)
        where TIndexKey : IComparable<TIndexKey>
    {
        public async Task UpdateAsync(T record, Pointer pointer, bool isInsert)
        {
            var indexKey = indexKeyExtractor(record);

            if (isUnique)
            {
                var uniqueIndex = (IUniqueTreeIndexManager<TIndexKey, Pointer>)index;
                await uniqueIndex.PutAsync(indexKey, pointer);
            }
            else
            {
                var duplicateIndex = (IDuplicateTreeIndexManager<TIndexKey, Pointer>)index;
                await duplicateIndex.PutAsync(indexKey, pointer);
            }
        }

        public async Task DeleteAsync(T record)
        {
            var indexKey = indexKeyExtractor(record);

            if (isUnique)
            {
                var uniqueIndex = (IUniqueTreeIndexManager<TIndexKey, Pointer>)index;
                await uniqueIndex.RemoveAsync(indexKey);
            }
            else
            {
                var duplicateIndex = (IDuplicateTreeIndexManager<TIndexKey, Pointer>)index;
                await duplicateIndex.RemoveAsync(indexKey);
            }
        }
    }
}