using System.Reflection;
using System.Runtime.CompilerServices;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Engine.Queries;
using SharpDB.Engine.Writes;
using SharpDB.Index.Manager;
using SharpDB.Index.Node;
using SharpDB.Index.Partition;
using SharpDB.Index.Session;
using SharpDB.Serialization;
using SharpDB.Storage.Page;
using Pointer = SharpDB.DataStructures.Pointer;

namespace SharpDB.Engine;

/// <summary>
///     Manages a collection of records with schema and indexes.
///     Coordinates CRUD operations, indexes, and relational validation hooks.
/// </summary>
public class CollectionManager<T, TKey> : IForeignKeyLookup, IVacuumable
    where T : class
    where TKey : IComparable<TKey>
{
    private readonly int _collectionId;
    private readonly string _collectionName;
    private readonly Schema _schema;
    private readonly IDataIOSession _dataSession;
    private readonly IIndexStorageManager _indexStorage;
    private readonly IUniqueQueryableIndex<TKey, Pointer> _primaryIndex;
    private readonly Func<T, TKey> _keyExtractor;
    private readonly Func<string, Task<IForeignKeyLookup?>>? _collectionResolver;
    private readonly ITransactionBoundary? _transactionBoundary;
    private readonly IDatabaseHeaderManager? _dbHeaderManager;
    private readonly int _partitionCount;
    private readonly IPartitionStrategy<TKey>? _partitionStrategy;
    private readonly IReadOnlyList<(PropertyInfo Property, string FieldName)> _autoIncrementFields;
    private readonly Dictionary<string, CompositeKeyISerializer> _compositeSerializers = new();
    private readonly Dictionary<string, CompositeKeySerializer> _compositeRawSerializers = new();

    private readonly Dictionary<string, ISecondaryIndexWrapper> _secondaryIndexes = new();
    private readonly IObjectSerializer _serializer;

    private readonly IHydratedQueryStore<TKey, T> _queryStore;
    private readonly ICollectionWriteOrchestrator<T, TKey> _writeOrchestrator;

    public CollectionManager(
        int collectionId,
        string collectionName,
        Schema schema,
        IDataIOSession dataSession,
        IIndexStorageManager indexStorage,
        IUniqueQueryableIndex<TKey, Pointer> primaryIndex,
        Func<T, TKey> keyExtractor,
        int partitionCount = 1,
        IPartitionStrategy<TKey>? partitionStrategy = null,
        Func<string, Task<IForeignKeyLookup?>>? collectionResolver = null,
        ITransactionBoundary? transactionBoundary = null,
        IDatabaseHeaderManager? dbHeaderManager = null)
    {
        _collectionId = collectionId;
        _collectionName = collectionName;
        _schema = schema;
        _dataSession = dataSession;
        _indexStorage = indexStorage;
        _primaryIndex = primaryIndex;
        _keyExtractor = keyExtractor;
        _partitionCount = partitionCount;
        _partitionStrategy = partitionStrategy;
        _collectionResolver = collectionResolver;
        _transactionBoundary = transactionBoundary;
        _dbHeaderManager = dbHeaderManager;
        _serializer = new BinaryObjectSerializer(schema);

        _autoIncrementFields = schema.Fields
            .Where(f => f.IsAutoIncrement)
            .Select(f => (typeof(T).GetProperty(f.Name)!, f.Name))
            .Where(x => x.Item1 != null)
            .ToList();

        _queryStore = new HydratedQueryStore<TKey, T>(
            primaryIndex,
            primaryIndex,
            dataSession,
            _serializer);

        _writeOrchestrator = new CollectionWriteOrchestrator<T, TKey>(
            dataSession,
            _serializer,
            primaryIndex,
            collectionId,
            keyExtractor,
            onSecondaryInsert: async (record, pointer, _) => await UpdateSecondaryIndexesForInsertAsync(record, pointer),
            onSecondaryDelete: async (record, _) => await UpdateSecondaryIndexesForDeleteAsync(record));
    }

    public string CollectionName => _collectionName;

    public string PrimaryKeyFieldName => _schema.PrimaryKey?.Name
        ?? throw new InvalidOperationException($"Collection '{_collectionName}' schema has no primary key");

    public async Task InsertAsync(T record)
    {
        if (_transactionBoundary != null)
        {
            await _transactionBoundary.ExecuteAsync(async tx =>
            {
                await InsertAsync(record, tx);
                return 0;
            });
            return;
        }

        await ApplyAutoIncrementAsync(record);
        await ValidateForeignKeysAsync(record);
        await _writeOrchestrator.InsertAsync(record);
    }

    public async Task InsertAsync(T record, ITransactionSession transaction)
    {
        await ApplyAutoIncrementAsync(record);

        await transaction.AcquireRangeWriteLockAsync(
            GetPrimaryRangeResourceType(),
            MinRangeKeyToken,
            MaxRangeKeyToken);

        await ValidateForeignKeysAsync(record);

        var key = _keyExtractor(record);
        var existing = await _primaryIndex.GetAsync(key);
        if (IsValidDataPointer(existing))
            throw new InvalidOperationException($"Record with key {key} already exists");

        var payload = _serializer.Serialize(record);
        var pointer = await transaction.WriteBytesAsync(null, payload, _collectionId);
        await _primaryIndex.PutAsync(key, pointer);
        await UpdateSecondaryIndexesForInsertAsync(record, pointer);

        await transaction.RegisterRollbackActionAsync(async () =>
        {
            await UpdateSecondaryIndexesForDeleteAsync(record);
            await _primaryIndex.RemoveAsync(key);
        });
    }

    public async Task<T?> SelectAsync(TKey primaryKey)
    {
        return await _queryStore.GetAsync(primaryKey);
    }

    public async Task<T?> SelectAsync(TKey primaryKey, ITransactionSession transaction)
    {
        var pointer = await _primaryIndex.GetAsync(primaryKey);
        if (!IsValidDataPointer(pointer))
            return null;

        var payload = await transaction.ReadBytesAsync(pointer);
        if (payload == null || payload.Length == 0)
            return null;

        return _serializer.Deserialize<T>(payload);
    }

    public async Task UpdateAsync(T record)
    {
        if (_transactionBoundary != null)
        {
            await _transactionBoundary.ExecuteAsync(async tx => await UpdateAsync(record, tx, upsert: false));
            return;
        }

        await ValidateForeignKeysAsync(record);
        await _writeOrchestrator.UpdateAsync(record);
    }

    public async Task<bool> UpdateAsync(T record, ITransactionSession transaction, bool upsert = false)
    {
        await transaction.AcquireRangeWriteLockAsync(
            GetPrimaryRangeResourceType(),
            MinRangeKeyToken,
            MaxRangeKeyToken);

        await ValidateForeignKeysAsync(record);

        var key = _keyExtractor(record);
        var currentPointer = await _primaryIndex.GetAsync(key);
        if (!IsValidDataPointer(currentPointer))
        {
            if (!upsert)
                throw new InvalidOperationException($"Record with key {key} not found");

            await InsertAsync(record, transaction);
            return true;
        }

        T? oldRecord = null;
        var oldPayload = await transaction.ReadBytesAsync(currentPointer);
        if (oldPayload != null && oldPayload.Length > 0)
            oldRecord = _serializer.Deserialize<T>(oldPayload);

        var newPayload = _serializer.Serialize(record);
        var newPointer = await transaction.WriteBytesAsync(currentPointer, newPayload);

        if (newPointer != currentPointer)
        {
            await _primaryIndex.RemoveAsync(key);
            await _primaryIndex.PutAsync(key, newPointer);
        }

        if (oldRecord != null)
            await UpdateSecondaryIndexesForDeleteAsync(oldRecord);

        await UpdateSecondaryIndexesForInsertAsync(record, newPointer);

        await transaction.RegisterRollbackActionAsync(async () =>
        {
            await UpdateSecondaryIndexesForDeleteAsync(record);

            if (oldRecord != null)
                await UpdateSecondaryIndexesForInsertAsync(oldRecord, currentPointer);

            await _primaryIndex.RemoveAsync(key);
            await _primaryIndex.PutAsync(key, currentPointer);
        });

        return true;
    }

    public async Task<bool> DeleteAsync(TKey primaryKey)
    {
        if (_transactionBoundary != null)
            return await _transactionBoundary.ExecuteAsync(async tx => await DeleteAsync(primaryKey, tx));

        return await _writeOrchestrator.DeleteAsync(primaryKey);
    }

    public async Task<bool> DeleteAsync(TKey primaryKey, ITransactionSession transaction)
    {
        await transaction.AcquireRangeWriteLockAsync(
            GetPrimaryRangeResourceType(),
            MinRangeKeyToken,
            MaxRangeKeyToken);

        var pointer = await _primaryIndex.GetAsync(primaryKey);
        if (!IsValidDataPointer(pointer))
            return false;

        T? oldRecord = null;
        var oldPayload = await transaction.ReadBytesAsync(pointer);
        if (oldPayload != null && oldPayload.Length > 0)
            oldRecord = _serializer.Deserialize<T>(oldPayload);

        // Write a tombstone version for WAL/rollback visibility.
        await transaction.WriteBytesAsync(pointer, Array.Empty<byte>());
        await _primaryIndex.RemoveAsync(primaryKey);

        if (oldRecord != null)
            await UpdateSecondaryIndexesForDeleteAsync(oldRecord);

        await transaction.RegisterRollbackActionAsync(async () =>
        {
            await _primaryIndex.PutAsync(primaryKey, pointer);

            if (oldRecord != null)
                await UpdateSecondaryIndexesForInsertAsync(oldRecord, pointer);
        });

        return true;
    }

    public async IAsyncEnumerable<T> ScanAsync()
    {
        // Ensure buffered mutations are visible during scan.
        await _dataSession.FlushAsync();

        await foreach (var dbObject in _dataSession.ScanAsync(_collectionId))
        {
            // Skip deleted objects (soft delete flag on page is the authoritative source)
            if (!dbObject.IsAlive)
                continue;

            // Zero-copy: read directly from the page buffer — avoids allocating a new byte[] per record.
            var record = _serializer.Deserialize<T>(dbObject.RawData, dbObject.DataOffset);
            yield return record;
        }
    }

    public async Task<int> CountAsync()
    {
        return await _primaryIndex.CountAsync();
    }

    // --- Helpers ---

    private static TKey VacuumMinKey()
    {
        var type = typeof(TKey);
        if (type == typeof(long)) return (TKey)(object)long.MinValue;
        if (type == typeof(int)) return (TKey)(object)int.MinValue;
        if (type == typeof(string)) return (TKey)(object)string.Empty;
        if (type == typeof(DateTime)) return (TKey)(object)DateTime.MinValue;
        if (type == typeof(decimal)) return (TKey)(object)decimal.MinValue;
        return default!;
    }

    private static TKey VacuumMaxKey()
    {
        var type = typeof(TKey);
        if (type == typeof(long)) return (TKey)(object)long.MaxValue;
        if (type == typeof(int)) return (TKey)(object)int.MaxValue;
        if (type == typeof(string)) return (TKey)(object)new string('\uffff', 255);
        if (type == typeof(DateTime)) return (TKey)(object)DateTime.MaxValue;
        if (type == typeof(decimal)) return (TKey)(object)decimal.MaxValue;
        return default!;
    }

    // --- Aggregates ---

    public async Task<decimal> SumAsync(Func<T, decimal> selector)
    {
        var sum = 0m;
        await foreach (var r in ScanAsync()) sum += selector(r);
        return sum;
    }

    public async Task<TValue?> MinAsync<TValue>(Func<T, TValue> selector) where TValue : IComparable<TValue>
    {
        TValue? min = default;
        var first = true;
        await foreach (var r in ScanAsync())
        {
            var v = selector(r);
            if (first || v.CompareTo(min!) < 0) { min = v; first = false; }
        }
        return min;
    }

    public async Task<TValue?> MaxAsync<TValue>(Func<T, TValue> selector) where TValue : IComparable<TValue>
    {
        TValue? max = default;
        var first = true;
        await foreach (var r in ScanAsync())
        {
            var v = selector(r);
            if (first || v.CompareTo(max!) > 0) { max = v; first = false; }
        }
        return max;
    }

    public async Task<double> AverageAsync(Func<T, double> selector)
    {
        var sum = 0.0;
        var count = 0;
        await foreach (var r in ScanAsync()) { sum += selector(r); count++; }
        return count == 0 ? 0.0 : sum / count;
    }

    public async Task<bool> ExistsByPrimaryKeyAsync(object key)
    {
        if (key is not TKey typedKey)
            return false;

        var pointer = await _primaryIndex.GetAsync(typedKey);
        return IsValidDataPointer(pointer);
    }

    /// <summary>
    /// Query records within a key range [minKey, maxKey] (inclusive).
    /// </summary>
    public IAsyncEnumerable<T> RangeQueryAsync(TKey minKey, TKey maxKey)
    {
        return _queryStore.QueryAsync(new QuerySpec<TKey>.Range(minKey, maxKey));
    }

    public async IAsyncEnumerable<T> RangeQueryAsync(TKey minKey, TKey maxKey, ITransactionSession transaction)
    {
        await transaction.AcquireRangeReadLockAsync(
            GetPrimaryRangeResourceType(),
            MinRangeKeyToken,
            MaxRangeKeyToken);

        await foreach (var item in _queryStore.QueryAsync(new QuerySpec<TKey>.Range(minKey, maxKey)))
            yield return item;
    }

    /// <summary>
    /// Query records with keys greater than the specified key.
    /// </summary>
    public IAsyncEnumerable<T> GreaterThanAsync(TKey key)
    {
        return _queryStore.QueryAsync(new QuerySpec<TKey>.GreaterThan(key));
    }

    public async IAsyncEnumerable<T> GreaterThanAsync(TKey key, ITransactionSession transaction)
    {
        await transaction.AcquireRangeReadLockAsync(
            GetPrimaryRangeResourceType(),
            MinRangeKeyToken,
            MaxRangeKeyToken);

        await foreach (var item in _queryStore.QueryAsync(new QuerySpec<TKey>.GreaterThan(key)))
            yield return item;
    }

    /// <summary>
    /// Query records with keys less than the specified key.
    /// </summary>
    public IAsyncEnumerable<T> LessThanAsync(TKey key)
    {
        return _queryStore.QueryAsync(new QuerySpec<TKey>.LessThan(key));
    }

    public async IAsyncEnumerable<T> LessThanAsync(TKey key, ITransactionSession transaction)
    {
        await transaction.AcquireRangeReadLockAsync(
            GetPrimaryRangeResourceType(),
            MinRangeKeyToken,
            MaxRangeKeyToken);

        await foreach (var item in _queryStore.QueryAsync(new QuerySpec<TKey>.LessThan(key)))
            yield return item;
    }

    /// <summary>
    /// Query first matching record by secondary index key.
    /// </summary>
    public async Task<T?> SelectBySecondaryIndexAsync<TIndexKey>(string fieldName, TIndexKey key)
        where TIndexKey : IComparable<TIndexKey>
    {
        await foreach (var record in SelectManyBySecondaryIndexAsync(fieldName, key))
            return record;

        return null;
    }

    /// <summary>
    /// Query all matching records by secondary index key.
    /// </summary>
    public async IAsyncEnumerable<T> SelectManyBySecondaryIndexAsync<TIndexKey>(string fieldName, TIndexKey key)
        where TIndexKey : IComparable<TIndexKey>
    {
        if (!_secondaryIndexes.TryGetValue(fieldName, out var wrapper))
            throw new InvalidOperationException($"Secondary index '{fieldName}' not found");

        var pointers = await wrapper.LookupPointersAsync(key!);
        await foreach (var record in LoadRecordsByPointersAsync(pointers))
            yield return record;
    }

    /// <summary>
    /// Range query on a unique secondary index.
    /// </summary>
    public async IAsyncEnumerable<T> RangeBySecondaryIndexAsync<TIndexKey>(
        string fieldName,
        TIndexKey minKey,
        TIndexKey maxKey)
        where TIndexKey : IComparable<TIndexKey>
    {
        if (!_secondaryIndexes.TryGetValue(fieldName, out var wrapper))
            throw new InvalidOperationException($"Secondary index '{fieldName}' not found");

        if (!wrapper.SupportsRange)
            throw new NotSupportedException($"Secondary index '{fieldName}' does not support range queries");

        await foreach (var record in LoadRecordsByPointersAsync(wrapper.RangePointersAsync(minKey!, maxKey!)))
            yield return record;
    }

    /// <summary>
    /// Hash-join using custom join key selectors on both sides.
    /// </summary>
    public async IAsyncEnumerable<(T Left, TRight Right)> InnerJoinAsync<TRight, TRightKey, TJoinKey>(
        CollectionManager<TRight, TRightKey> rightCollection,
        Func<T, TJoinKey> leftJoinKey,
        Func<TRight, TJoinKey> rightJoinKey)
        where TRight : class
        where TRightKey : IComparable<TRightKey>
        where TJoinKey : notnull
    {
        var rightLookup = new Dictionary<TJoinKey, List<TRight>>();

        await foreach (var right in rightCollection.ScanAsync())
        {
            var key = rightJoinKey(right);
            if (!rightLookup.TryGetValue(key, out var bucket))
            {
                bucket = [];
                rightLookup[key] = bucket;
            }

            bucket.Add(right);
        }

        await foreach (var left in ScanAsync())
        {
            var key = leftJoinKey(left);
            if (!rightLookup.TryGetValue(key, out var matches))
                continue;

            foreach (var right in matches)
                yield return (left, right);
        }
    }

    /// <summary>
    /// Optimized join where left selector references right primary key.
    /// </summary>
    public async IAsyncEnumerable<(T Left, TRight Right)> InnerJoinByRightPrimaryKeyAsync<TRight, TRightKey>(
        CollectionManager<TRight, TRightKey> rightCollection,
        Func<T, TRightKey> leftKeySelector)
        where TRight : class
        where TRightKey : IComparable<TRightKey>
    {
        await foreach (var left in ScanAsync())
        {
            var right = await rightCollection.SelectAsync(leftKeySelector(left));
            if (right != null)
                yield return (left, right);
        }
    }

    public async Task FlushAsync()
    {
        await _dataSession.FlushAsync();
        await _primaryIndex.FlushAsync();
    }

    /// <summary>
    /// Reclaims disk space by collecting all alive records, truncating storage and indexes,
    /// then re-inserting everything. This is a blocking maintenance operation.
    /// </summary>
    public async Task VacuumAsync()
    {
        // 1. Collect all alive records.
        // Read from raw storage pages (fast sequential scan) and cross-check each key
        // against the primary index to exclude MVCC-deleted records (transactional deletes
        // remove from the index but do not immediately update the raw-storage IsAlive flag).
        var records = new List<T>();
        await foreach (var dbObject in _dataSession.ScanAsync(_collectionId))
        {
            if (!dbObject.IsAlive) continue;
            var record = _serializer.Deserialize<T>(dbObject.RawData, dbObject.DataOffset);
            var key = _keyExtractor(record);
            var pointer = await _primaryIndex.GetAsync(key);
            if (IsValidDataPointer(pointer))
                records.Add(record);
        }

        // 2. Flush everything pending to disk before destructive truncation.
        await _dataSession.FlushAsync();
        await _primaryIndex.FlushAsync();

        // 3. Truncate data storage (deletes file, clears cache)
        await _dataSession.TruncateCollectionAsync(_collectionId);

        // 4. Reset primary index (clears in-memory buffer, truncates index file)
        await _primaryIndex.ResetAsync();

        // 5. Reset all secondary indexes
        foreach (var wrapper in _secondaryIndexes.Values)
            await wrapper.ResetAsync();

        // 6. Re-insert all records (rebuilds all indexes automatically).
        // Use the write orchestrator directly to bypass auto-increment (IDs are already set)
        // and the transaction boundary (vacuum is a maintenance operation on a stable snapshot).
        foreach (var record in records)
            await _writeOrchestrator.InsertAsync(record);
    }

    public async Task CreateSecondaryIndexAsync<TIndexKey>(
        string fieldName,
        Func<T, TIndexKey> indexKeyExtractor,
        bool isUnique = false)
        where TIndexKey : IComparable<TIndexKey>
    {
        var field = _schema.GetField(fieldName);
        if (field == null)
            throw new InvalidOperationException($"Field {fieldName} not found");

        if (_secondaryIndexes.ContainsKey(fieldName))
            throw new InvalidOperationException($"Index on {fieldName} already exists");

        if (_partitionCount > 1 && _partitionStrategy != null)
        {
            _secondaryIndexes[fieldName] = CreateLocalSecondaryIndex(indexKeyExtractor, isUnique);
            return;
        }

        var indexId = _collectionId * 1000 + _secondaryIndexes.Count + 1;

        // Create appropriate index type
        if (isUnique)
        {
            var index = new BPlusTreeIndexManager<TIndexKey, Pointer>(
                _indexStorage,
                indexId,
                128);

            var queryable = CreateQueryableSecondaryIndex(indexId, index);

            _secondaryIndexes[fieldName] = new SecondaryIndexWrapper<TIndexKey>(
                indexKeyExtractor,
                uniqueIndex: index,
                duplicateIndex: null,
                queryableIndex: queryable);
        }
        else
        {
            var index = new DuplicateBPlusTreeIndexManager<TIndexKey, Pointer>(
                _indexStorage,
                indexId,
                128,
                CreateSerializer<TIndexKey>(),
                new PointerSerializer());

            _secondaryIndexes[fieldName] = new SecondaryIndexWrapper<TIndexKey>(
                indexKeyExtractor,
                uniqueIndex: null,
                duplicateIndex: index,
                queryableIndex: null);
        }
    }

    /// <summary>
    /// Creates a composite secondary index on multiple fields.
    /// </summary>
    public Task CreateCompositeIndexAsync(
        string indexName,
        (string FieldName, FieldType FieldType, int Size)[] fields,
        Func<T, object[]> valuesExtractor,
        bool isUnique = false)
    {
        if (_secondaryIndexes.ContainsKey(indexName))
            throw new InvalidOperationException($"Index '{indexName}' already exists");

        var keyFields = fields.Select(f => (f.FieldName, f.FieldType, f.Size)).ToList();
        var rawSerializer = new CompositeKeySerializer(keyFields);
        var keyISerializer = new CompositeKeyISerializer(rawSerializer);
        _compositeSerializers[indexName] = keyISerializer;
        _compositeRawSerializers[indexName] = rawSerializer;

        Func<T, CompositeKey> keyExtractor = record =>
        {
            var values = valuesExtractor(record);
            var bytes = rawSerializer.Serialize(values);
            return new CompositeKey(bytes);
        };

        var indexId = _collectionId * 1000 + _secondaryIndexes.Count + 1;

        if (isUnique)
        {
            var index = new BPlusTreeIndexManager<CompositeKey, Pointer>(
                _indexStorage, indexId, 128, keyISerializer, new PointerSerializer());
            var queryable = CreateQueryableSecondaryIndex(indexId, index, keyISerializer);
            _secondaryIndexes[indexName] = new SecondaryIndexWrapper<CompositeKey>(
                keyExtractor, uniqueIndex: index, duplicateIndex: null, queryableIndex: queryable);
        }
        else
        {
            var index = new DuplicateBPlusTreeIndexManager<CompositeKey, Pointer>(
                _indexStorage, indexId, 128, keyISerializer, new PointerSerializer());
            _secondaryIndexes[indexName] = new SecondaryIndexWrapper<CompositeKey>(
                keyExtractor, uniqueIndex: null, duplicateIndex: index, queryableIndex: null);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Lookup by composite index using exact key values.
    /// </summary>
    public async Task<T?> SelectByCompositeIndexAsync(string indexName, object[] keyValues)
    {
        if (!_compositeRawSerializers.TryGetValue(indexName, out var rawSerializer))
            throw new InvalidOperationException($"Composite index '{indexName}' not found");

        var key = new CompositeKey(rawSerializer.Serialize(keyValues));
        await foreach (var record in SelectManyBySecondaryIndexAsync(indexName, key))
            return record;
        return null;
    }

    /// <summary>
    /// Range query on a composite index.
    /// </summary>
    public IAsyncEnumerable<T> RangeByCompositeIndexAsync(string indexName, object[] minValues, object[] maxValues)
    {
        if (!_compositeRawSerializers.TryGetValue(indexName, out var rawSerializer))
            throw new InvalidOperationException($"Composite index '{indexName}' not found");

        var minKey = new CompositeKey(rawSerializer.Serialize(minValues));
        var maxKey = new CompositeKey(rawSerializer.Serialize(maxValues));
        return RangeBySecondaryIndexAsync(indexName, minKey, maxKey);
    }

    private IUniqueQueryableIndex<TIndexKey, Pointer> CreateQueryableSecondaryIndex<TIndexKey>(
        int indexId,
        IUniqueTreeIndexManager<TIndexKey, Pointer> uniqueIndex,
        ISerializer<TIndexKey> keySerializer)
        where TIndexKey : IComparable<TIndexKey>
    {
        var nodeFactory = new BPlusTreeNodeFactory<TIndexKey, Pointer>(
            keySerializer,
            new PointerSerializer(),
            128);

        var querySession = new BufferedIndexIOSession<TIndexKey>(
            _indexStorage,
            new ObjectNodeFactoryAdapter<TIndexKey, Pointer>(nodeFactory, allowCreateInternalNode: false),
            indexId);

        return new UniqueQueryableIndexDecorator<TIndexKey, Pointer>(
            uniqueIndex,
            querySession,
            _indexStorage,
            indexId);
    }

    private ISecondaryIndexWrapper CreateLocalSecondaryIndex<TIndexKey>(
        Func<T, TIndexKey> indexKeyExtractor,
        bool isUnique)
        where TIndexKey : IComparable<TIndexKey>
    {
        // Base ID for this secondary index's partitions: C*10000 + (S+1)*100 + P
        var secondarySlot = _secondaryIndexes.Count + 1;
        var baseId = _collectionId * 10000 + secondarySlot * 100;

        if (isUnique)
        {
            var uniqueIndexes = new IUniqueTreeIndexManager<TIndexKey, Pointer>[_partitionCount];
            var queryableIndexes = new IUniqueQueryableIndex<TIndexKey, Pointer>[_partitionCount];

            for (var p = 0; p < _partitionCount; p++)
            {
                var indexId = baseId + p;
                var index = new BPlusTreeIndexManager<TIndexKey, Pointer>(_indexStorage, indexId, 128);
                uniqueIndexes[p] = index;
                queryableIndexes[p] = CreateQueryableSecondaryIndex(indexId, index);
            }

            return new LocalPartitionedSecondaryIndexWrapper<TIndexKey>(
                _keyExtractor, indexKeyExtractor, _partitionStrategy!, _partitionCount,
                uniqueIndexes: uniqueIndexes, duplicateIndexes: null, queryableIndexes: queryableIndexes);
        }
        else
        {
            var duplicateIndexes = new IDuplicateTreeIndexManager<TIndexKey, Pointer>[_partitionCount];

            for (var p = 0; p < _partitionCount; p++)
            {
                var indexId = baseId + p;
                duplicateIndexes[p] = new DuplicateBPlusTreeIndexManager<TIndexKey, Pointer>(
                    _indexStorage, indexId, 128, CreateSerializer<TIndexKey>(), new PointerSerializer());
            }

            return new LocalPartitionedSecondaryIndexWrapper<TIndexKey>(
                _keyExtractor, indexKeyExtractor, _partitionStrategy!, _partitionCount,
                uniqueIndexes: null, duplicateIndexes: duplicateIndexes, queryableIndexes: null);
        }
    }

    private IUniqueQueryableIndex<TIndexKey, Pointer> CreateQueryableSecondaryIndex<TIndexKey>(
        int indexId,
        IUniqueTreeIndexManager<TIndexKey, Pointer> uniqueIndex)
        where TIndexKey : IComparable<TIndexKey>
    {
        var nodeFactory = new BPlusTreeNodeFactory<TIndexKey, Pointer>(
            CreateSerializer<TIndexKey>(),
            new PointerSerializer(),
            128);

        var querySession = new BufferedIndexIOSession<TIndexKey>(
            _indexStorage,
            new ObjectNodeFactoryAdapter<TIndexKey, Pointer>(nodeFactory, allowCreateInternalNode: false),
            indexId);

        return new UniqueQueryableIndexDecorator<TIndexKey, Pointer>(
            uniqueIndex,
            querySession,
            _indexStorage,
            indexId);
    }

    private async Task ApplyAutoIncrementAsync(T record)
    {
        if (_autoIncrementFields.Count == 0 || _dbHeaderManager == null)
            return;

        foreach (var (prop, fieldName) in _autoIncrementFields)
        {
            var next = await _dbHeaderManager.GetNextSequenceValueAsync(_collectionId, fieldName);
            if (prop.PropertyType == typeof(long))
                prop.SetValue(record, next);
            else if (prop.PropertyType == typeof(int))
                prop.SetValue(record, (int)next);
        }
    }

    private async ValueTask UpdateSecondaryIndexesForInsertAsync(T record, Pointer pointer)
    {
        if (_secondaryIndexes.Count == 0)
            return;

        foreach (var indexWrapper in _secondaryIndexes.Values)
            await indexWrapper.UpdateAsync(record, pointer);
    }

    private async ValueTask UpdateSecondaryIndexesForDeleteAsync(T record)
    {
        if (_secondaryIndexes.Count == 0)
            return;

        foreach (var indexWrapper in _secondaryIndexes.Values)
            await indexWrapper.DeleteAsync(record);
    }

    private async Task ValidateForeignKeysAsync(T record)
    {
        if (_schema.ForeignKeys.Count == 0)
            return;

        if (_collectionResolver == null)
            throw new InvalidOperationException(
                $"Collection '{_collectionName}' has foreign keys but no resolver is configured");

        var recordType = typeof(T);

        foreach (var fk in _schema.ForeignKeys)
        {
            var property = recordType.GetProperty(fk.FieldName)
                ?? throw new InvalidOperationException(
                    $"Foreign key field '{fk.FieldName}' is not present on record type '{recordType.Name}'");

            var value = property.GetValue(record);
            if (value == null)
            {
                if (fk.AllowNull)
                    continue;

                throw new InvalidOperationException(
                    $"Foreign key field '{fk.FieldName}' cannot be null in collection '{_collectionName}'");
            }

            var targetCollection = await _collectionResolver(fk.ReferencedCollection)
                ?? throw new InvalidOperationException(
                    $"Referenced collection '{fk.ReferencedCollection}' is not loaded");

            if (!string.Equals(targetCollection.PrimaryKeyFieldName, fk.ReferencedField, StringComparison.Ordinal))
                throw new InvalidOperationException(
                    $"Foreign key '{fk.FieldName}' points to '{fk.ReferencedCollection}.{fk.ReferencedField}', " +
                    $"but only primary-key references are currently supported ('{targetCollection.PrimaryKeyFieldName}')");

            var exists = await targetCollection.ExistsByPrimaryKeyAsync(value);
            if (!exists)
                throw new InvalidOperationException(
                    $"Foreign key violation on '{fk.FieldName}': value '{value}' was not found in " +
                    $"'{fk.ReferencedCollection}.{fk.ReferencedField}'");
        }
    }

    private async IAsyncEnumerable<T> LoadRecordsByPointersAsync(IEnumerable<Pointer> pointers)
    {
        foreach (var pointer in pointers)
        {
            var dbObject = await _dataSession.SelectAsync(pointer);
            if (dbObject == null)
                continue;

            yield return _serializer.Deserialize<T>(dbObject.Data);
        }
    }

    private async IAsyncEnumerable<T> LoadRecordsByPointersAsync(IAsyncEnumerable<Pointer> pointers)
    {
        await foreach (var pointer in pointers)
        {
            var dbObject = await _dataSession.SelectAsync(pointer);
            if (dbObject == null)
                continue;

            yield return _serializer.Deserialize<T>(dbObject.Data);
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

    private static bool IsValidDataPointer(Pointer pointer)
    {
        return pointer.Type == Pointer.TypeData && pointer.Position > 0;
    }

    private Pointer ToPointer(DBObject dbObject)
    {
        var pageSize = dbObject.Page.Data.Length;
        var position = (long)dbObject.Page.PageNumber * pageSize + dbObject.Begin;
        return new Pointer(Pointer.TypeData, position, _collectionId);
    }

    private string GetPrimaryRangeResourceType()
    {
        return $"pk-range:{_collectionId}";
    }

    private const string MinRangeKeyToken = "__MIN__";
    private const string MaxRangeKeyToken = "__MAX__";

    private interface ISecondaryIndexWrapper
    {
        Task UpdateAsync(T record, Pointer pointer);
        Task DeleteAsync(T record);
        Task<IReadOnlyList<Pointer>> LookupPointersAsync(object key);
        IAsyncEnumerable<Pointer> RangePointersAsync(object minKey, object maxKey);
        bool SupportsRange { get; }
        Task ResetAsync();
    }

    private sealed class SecondaryIndexWrapper<TIndexKey>(
        Func<T, TIndexKey> indexKeyExtractor,
        IUniqueTreeIndexManager<TIndexKey, Pointer>? uniqueIndex,
        IDuplicateTreeIndexManager<TIndexKey, Pointer>? duplicateIndex,
        IUniqueQueryableIndex<TIndexKey, Pointer>? queryableIndex)
        : ISecondaryIndexWrapper
        where TIndexKey : IComparable<TIndexKey>
    {
        public bool SupportsRange => queryableIndex != null;

        public async Task UpdateAsync(T record, Pointer pointer)
        {
            var indexKey = indexKeyExtractor(record);

            if (uniqueIndex != null)
            {
                await uniqueIndex.PutAsync(indexKey, pointer);
                return;
            }

            if (duplicateIndex != null)
            {
                await duplicateIndex.PutAsync(indexKey, pointer);
                return;
            }

            throw new InvalidOperationException("Secondary index wrapper is not configured");
        }

        public async Task DeleteAsync(T record)
        {
            var indexKey = indexKeyExtractor(record);

            if (uniqueIndex != null)
            {
                await uniqueIndex.RemoveAsync(indexKey);
                return;
            }

            if (duplicateIndex != null)
            {
                await duplicateIndex.RemoveAsync(indexKey);
                return;
            }

            throw new InvalidOperationException("Secondary index wrapper is not configured");
        }

        public async Task<IReadOnlyList<Pointer>> LookupPointersAsync(object key)
        {
            var typedKey = EnsureTypedKey(key);

            if (uniqueIndex != null)
            {
                var pointer = await uniqueIndex.GetAsync(typedKey);
                if (!IsValidDataPointer(pointer))
                    return [];

                return [pointer];
            }

            if (duplicateIndex != null)
            {
                var pointers = await duplicateIndex.GetAllAsync(typedKey);
                return pointers.Where(IsValidDataPointer).ToList();
            }

            throw new InvalidOperationException("Secondary index wrapper is not configured");
        }

        public IAsyncEnumerable<Pointer> RangePointersAsync(object minKey, object maxKey)
        {
            if (queryableIndex == null)
                throw new NotSupportedException("Range query is only supported for unique secondary indexes");

            var typedMin = EnsureTypedKey(minKey);
            var typedMax = EnsureTypedKey(maxKey);

            return RangePointersCoreAsync(typedMin, typedMax);
        }

        private async IAsyncEnumerable<Pointer> RangePointersCoreAsync(TIndexKey minKey, TIndexKey maxKey)
        {
            if (queryableIndex == null)
                yield break;

            await foreach (var kv in queryableIndex.RangeAsync(minKey, maxKey))
                if (IsValidDataPointer(kv.Value))
                    yield return kv.Value;
        }

        public Task ResetAsync()
        {
            if (queryableIndex != null)
                return queryableIndex.ResetAsync();
            if (uniqueIndex != null)
                return uniqueIndex.ResetAsync();
            if (duplicateIndex != null)
                return duplicateIndex.ResetAsync();
            return Task.CompletedTask;
        }

        private static TIndexKey EnsureTypedKey(object key)
        {
            if (key is TIndexKey typed)
                return typed;

            throw new InvalidOperationException(
                $"Secondary index key type mismatch. Expected {typeof(TIndexKey).Name}, got {key.GetType().Name}");
        }

        private static bool IsValidDataPointer(Pointer pointer)
        {
            return pointer.Type == Pointer.TypeData && pointer.Position > 0;
        }
    }

    private sealed class LocalPartitionedSecondaryIndexWrapper<TIndexKey>(
        Func<T, TKey> primaryKeyExtractor,
        Func<T, TIndexKey> indexKeyExtractor,
        IPartitionStrategy<TKey> strategy,
        int partitionCount,
        IUniqueTreeIndexManager<TIndexKey, Pointer>[]? uniqueIndexes,
        IDuplicateTreeIndexManager<TIndexKey, Pointer>[]? duplicateIndexes,
        IUniqueQueryableIndex<TIndexKey, Pointer>[]? queryableIndexes)
        : ISecondaryIndexWrapper
        where TIndexKey : IComparable<TIndexKey>
    {
        public bool SupportsRange => queryableIndexes != null;

        public async Task UpdateAsync(T record, Pointer pointer)
        {
            var p = GetPartition(record);
            var indexKey = indexKeyExtractor(record);

            if (uniqueIndexes != null)
                await uniqueIndexes[p].PutAsync(indexKey, pointer);
            else if (duplicateIndexes != null)
                await duplicateIndexes[p].PutAsync(indexKey, pointer);
        }

        public async Task DeleteAsync(T record)
        {
            var p = GetPartition(record);
            var indexKey = indexKeyExtractor(record);

            if (uniqueIndexes != null)
                await uniqueIndexes[p].RemoveAsync(indexKey);
            else if (duplicateIndexes != null)
                await duplicateIndexes[p].RemoveAsync(indexKey);
        }

        public async Task<IReadOnlyList<Pointer>> LookupPointersAsync(object key)
        {
            var typedKey = EnsureTypedKey(key);

            if (uniqueIndexes != null)
            {
                var tasks = uniqueIndexes.Select(idx => idx.GetAsync(typedKey)).ToArray();
                var results = await Task.WhenAll(tasks);
                return results.Where(IsValidDataPointer).ToList();
            }

            if (duplicateIndexes != null)
            {
                var tasks = duplicateIndexes.Select(idx => idx.GetAllAsync(typedKey)).ToArray();
                var batches = await Task.WhenAll(tasks);
                return batches.SelectMany(b => b).Where(IsValidDataPointer).ToList();
            }

            return [];
        }

        public IAsyncEnumerable<Pointer> RangePointersAsync(object minKey, object maxKey)
        {
            if (queryableIndexes == null)
                throw new NotSupportedException("Range query is only supported for unique secondary indexes");

            var typedMin = EnsureTypedKey(minKey);
            var typedMax = EnsureTypedKey(maxKey);
            return RangePointersCoreAsync(typedMin, typedMax);
        }

        private async IAsyncEnumerable<Pointer> RangePointersCoreAsync(TIndexKey minKey, TIndexKey maxKey)
        {
            var tasks = queryableIndexes!
                .Select(idx => CollectRangeKvAsync(idx, minKey, maxKey))
                .ToArray();
            var batches = await Task.WhenAll(tasks);
            foreach (var kv in batches.SelectMany(b => b).OrderBy(kv => kv.Key))
                yield return kv.Value;
        }

        public Task ResetAsync()
        {
            if (queryableIndexes != null)
                return Task.WhenAll(queryableIndexes.Select(i => i.ResetAsync()));
            if (uniqueIndexes != null)
                return Task.WhenAll(uniqueIndexes.Select(i => i.ResetAsync()));
            if (duplicateIndexes != null)
                return Task.WhenAll(duplicateIndexes.Select(i => i.ResetAsync()));
            return Task.CompletedTask;
        }

        private static async Task<List<KeyValue<TIndexKey, Pointer>>> CollectRangeKvAsync(
            IUniqueQueryableIndex<TIndexKey, Pointer> idx, TIndexKey min, TIndexKey max)
        {
            var results = new List<KeyValue<TIndexKey, Pointer>>();
            await foreach (var kv in idx.RangeAsync(min, max))
                if (IsValidDataPointer(kv.Value))
                    results.Add(kv);
            return results;
        }

        private int GetPartition(T record) =>
            strategy.GetPartition(primaryKeyExtractor(record), partitionCount);

        private static TIndexKey EnsureTypedKey(object key)
        {
            if (key is TIndexKey typed) return typed;
            throw new InvalidOperationException(
                $"Secondary index key type mismatch. Expected {typeof(TIndexKey).Name}, got {key.GetType().Name}");
        }

        private static bool IsValidDataPointer(Pointer pointer) =>
            pointer.Type == Pointer.TypeData && pointer.Position > 0;
    }
}
