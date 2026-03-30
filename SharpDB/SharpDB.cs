using Serilog;
using SharpDB.Configuration;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Engine;
using SharpDB.Engine.Concurrency;
using SharpDB.Engine.Transaction;
using SharpDB.Index.Manager;
using SharpDB.Index.Node;
using SharpDB.Index.Partition;
using SharpDB.Index.Session;
using SharpDB.Serialization;
using SharpDB.Storage.Database;
using SharpDB.Storage.FilePool;
using SharpDB.Storage.Header;
using SharpDB.Storage.Index;
using SharpDB.Storage.Page;
using SharpDB.Storage.Sessions;
using SharpDB.WAL;

namespace SharpDB;

public class SharpDB : IDisposable
{
    private readonly Dictionary<int, object> _collections = new();
    private readonly Dictionary<string, object> _collectionsByName = new(StringComparer.Ordinal);
    private readonly EngineConfig _config;
    private readonly string _basePath;
    private readonly IDatabaseHeaderManager _dbHeaderManager;
    private readonly IDatabaseStorageManager _dbStorage;
    private readonly IFileHandlerPool _filePool;
    private readonly IIndexStorageManager _indexStorage;
    private readonly ILogger _logger = Log.ForContext<SharpDB>();
    private readonly ITransactionBoundary _transactionBoundary;
    private readonly ITransactionManager _transactionManager;
    private readonly WALManager? _walManager;
    private readonly object _checkpointSync = new();
    private Task<long>? _activeCheckpointTask;
    private bool _disposed;

    public SharpDB(string basePath, EngineConfig? config = null)
    {
        _config = config ?? EngineConfig.Default;
        _basePath = basePath;

        Directory.CreateDirectory(basePath);

        // Initialize components
        _filePool = new FileHandlerPool(_logger, _config);
        IPageManager pageManager = new PageManager(basePath, _filePool, _config.PageSize, _config.Cache.PageCacheSize, _config.SyncOnWrite);
        _dbHeaderManager = new DatabaseHeaderManager(basePath);
        _dbStorage = new DiskPageDatabaseStorageManager(pageManager, _logger, _dbHeaderManager, _config);
        _indexStorage = new DiskPageFileIndexStorageManager(basePath, _logger, _filePool, _config.SyncOnWrite);

        // Initialize WAL Manager after storage is available so recovery can apply data changes.
        if (_config.EnableWAL)
        {
            _walManager = new WALManager(basePath, _dbStorage, _config.WALMaxFileSize);

            // Perform recovery from WAL
            var recoveryTask = _walManager.RecoverAsync();
            recoveryTask.Wait(); // Synchronously wait for recovery to complete
            var recoveryResult = recoveryTask.Result;

            _logger.Information("WAL recovery completed. Committed: {Committed}, Aborted: {Aborted}, Rolled back: {RolledBack}",
                recoveryResult.CommittedTransactions,
                recoveryResult.AbortedTransactions,
                recoveryResult.UnfinishedTransactions);
        }

        var lockManager = new LockManager();
        var versionManager = new VersionManager(_dbStorage);
        var serializer = new JsonObjectSerializer();

        _transactionManager = new TransactionManager(
            lockManager,
            versionManager,
            serializer,
            _walManager,
            _config.EnableWAL,
            _config.WALAutoCheckpoint,
            _config.WALCheckpointInterval,
            CreateCheckpointAsync,
            indexFlushAsync: () => _indexStorage.FlushAsync());
        _transactionBoundary = new TransactionBoundary(_transactionManager);
    }

    public ITransactionBoundary Transactions => _transactionBoundary;

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            FlushAsync().Wait();
        }
        catch (ObjectDisposedException)
        {
            // Best-effort flush during dispose.
        }
        catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is ObjectDisposedException))
        {
            // Best-effort flush during dispose.
        }

        foreach (var collection in _collections.Values)
            if (collection is IDisposable disposable)
                disposable.Dispose();

        _transactionManager?.Dispose();
        _walManager?.Dispose();  // Dispose WAL manager to flush pending logs
        _dbStorage?.Dispose();
        _indexStorage?.Dispose();
        _filePool?.Dispose();
    }

    public async Task<CollectionManager<T, TKey>> CreateCollectionAsync<T, TKey>(
        string name,
        Schema schema,
        Func<T, TKey> keyExtractor,
        int partitionCount = 1)
        where T : class
        where TKey : IComparable<TKey>
    {
        var collectionId = await _dbHeaderManager.CreateCollectionAsync(name, schema);
        return CreateCollectionManager(collectionId, name, schema, keyExtractor, partitionCount);
    }

    public async Task<CollectionManager<T, TKey>> CreateCollectionAsync<T, TKey>(
        string name,
        Schema schema,
        Func<T, TKey> keyExtractor,
        RangePartitionStrategy<TKey> partitionStrategy)
        where T : class
        where TKey : IComparable<TKey>
    {
        var collectionId = await _dbHeaderManager.CreateCollectionAsync(name, schema);
        return CreateCollectionManager(collectionId, name, schema, keyExtractor, partitionStrategy);
    }

    private CollectionManager<T, TKey> CreateCollectionManager<T, TKey>(
        int collectionId,
        string collectionName,
        Schema schema,
        Func<T, TKey> keyExtractor,
        RangePartitionStrategy<TKey> partitionStrategy)
        where T : class
        where TKey : IComparable<TKey>
        => CreateCollectionManagerCore(collectionId, collectionName, schema, keyExtractor,
            partitionStrategy.PartitionCount, partitionStrategy);

    private CollectionManager<T, TKey> CreateCollectionManager<T, TKey>(
        int collectionId,
        string collectionName,
        Schema schema,
        Func<T, TKey> keyExtractor,
        int partitionCount = 1)
        where T : class
        where TKey : IComparable<TKey>
        => CreateCollectionManagerCore(collectionId, collectionName, schema, keyExtractor,
            partitionCount, partitionCount > 1 ? new HashPartitionStrategy<TKey>() : null);

    private CollectionManager<T, TKey> CreateCollectionManagerCore<T, TKey>(
        int collectionId,
        string collectionName,
        Schema schema,
        Func<T, TKey> keyExtractor,
        int partitionCount,
        IPartitionStrategy<TKey>? partitionStrategy)
        where T : class
        where TKey : IComparable<TKey>
    {
        if (_collections.TryGetValue(collectionId, out var existing))
        {
            _collectionsByName[collectionName] = existing;
            return (CollectionManager<T, TKey>)existing;
        }

        var primaryIndex = partitionCount <= 1
            ? BuildSingleIndex<TKey>(collectionId)
            : BuildPartitionedIndex<TKey>(collectionId, partitionCount, partitionStrategy!);

        var dataSession = new BufferedDataIOSession(_dbStorage, _config);

        var collection = new CollectionManager<T, TKey>(
            collectionId,
            collectionName,
            schema,
            dataSession,
            _indexStorage,
            primaryIndex,
            keyExtractor,
            partitionCount: partitionCount,
            partitionStrategy: partitionStrategy,
            ResolveCollectionAsync,
            _transactionBoundary,
            dbHeaderManager: _dbHeaderManager
        );

        _collections[collectionId] = collection;
        _collectionsByName[collectionName] = collection;

        return collection;
    }

    private IUniqueQueryableIndex<TKey, Pointer> BuildSingleIndex<TKey>(int collectionId)
        where TKey : IComparable<TKey>
    {
        var keySerializer = CreateSerializer<TKey>();
        var valueSerializer = new PointerSerializer();

        var index = new BPlusTreeIndexManager<TKey, Pointer>(_indexStorage, collectionId, _config.BTreeDegree);
        var nodeFactory = CreateNodeFactory<TKey>(keySerializer, valueSerializer, _config.BTreeDegree);
        var session = new BufferedIndexIOSession<TKey>(_indexStorage, nodeFactory, collectionId);

        return new UniqueQueryableIndexDecorator<TKey, Pointer>(index, session, _indexStorage, collectionId);
    }

    private IUniqueQueryableIndex<TKey, Pointer> BuildPartitionedIndex<TKey>(
        int collectionId, int partitionCount, IPartitionStrategy<TKey> strategy)
        where TKey : IComparable<TKey>
    {
        var keySerializer = CreateSerializer<TKey>();
        var valueSerializer = new PointerSerializer();
        var nodeFactory = CreateNodeFactory<TKey>(keySerializer, valueSerializer, _config.BTreeDegree);

        var partitions = new IUniqueQueryableIndex<TKey, Pointer>[partitionCount];
        for (var i = 0; i < partitionCount; i++)
        {
            var indexId = collectionId * 10000 + i;
            var index = new BPlusTreeIndexManager<TKey, Pointer>(_indexStorage, indexId, _config.BTreeDegree);
            var session = new BufferedIndexIOSession<TKey>(_indexStorage, nodeFactory, indexId);
            partitions[i] = new UniqueQueryableIndexDecorator<TKey, Pointer>(index, session, _indexStorage, indexId);
        }

        return new PartitionedIndexManager<TKey>(partitions, strategy);
    }

    public async Task<CollectionManager<T, TKey>> GetCollectionAsync<T, TKey>(
        string name,
        Func<T, TKey> keyExtractor,
        int partitionCount = 1)
        where T : class
        where TKey : IComparable<TKey>
    {
        var collections = await _dbHeaderManager.GetCollectionsAsync();
        var collectionInfo = collections.FirstOrDefault(c => c.Name == name);

        if (collectionInfo == null)
            throw new InvalidOperationException($"Collection '{name}' not found");

        if (_collections.TryGetValue(collectionInfo.CollectionId, out var existing))
        {
            _collectionsByName[collectionInfo.Name] = existing;
            return (CollectionManager<T, TKey>)existing;
        }

        var schema = await _dbHeaderManager.GetSchemaAsync(collectionInfo.CollectionId);
        return CreateCollectionManager(collectionInfo.CollectionId, collectionInfo.Name, schema!, keyExtractor, partitionCount);
    }

    public async Task<ITransaction> BeginTransactionAsync(IsolationLevel level = IsolationLevel.ReadCommitted)
    {
        return await _transactionManager.BeginTransactionAsync(level);
    }

    public async Task CommitTransactionAsync(ITransaction transaction)
    {
        await _transactionManager.CommitAsync(transaction);
    }

    public async Task RollbackTransactionAsync(ITransaction transaction)
    {
        await _transactionManager.RollbackAsync(transaction);
    }

    public async Task FlushAsync()
    {
        await _dbStorage.FlushAsync();
        await _indexStorage.FlushAsync();
        await _filePool.FlushAllAsync();
        _walManager?.Flush();
    }

    /// <summary>
    /// Creates a consistent online backup by flushing all pending writes then copying all database files.
    /// </summary>
    public async Task BackupAsync(string targetPath)
    {
        if (string.IsNullOrWhiteSpace(targetPath))
            throw new ArgumentException("Target path must not be empty", nameof(targetPath));

        await FlushAsync();
        if (_walManager != null)
            await CreateCheckpointAsync();

        Directory.CreateDirectory(targetPath);
        foreach (var srcFile in Directory.GetFiles(_basePath, "*", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(_basePath, srcFile);
            var dst = Path.Combine(targetPath, rel);
            Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
            // Open with FileShare.ReadWrite so we can read files already open by the file pool.
            await using var src = new FileStream(srcFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            await using var dstFs = new FileStream(dst, FileMode.Create, FileAccess.Write, FileShare.None);
            await src.CopyToAsync(dstFs);
        }

        _logger.Information("Backup created at {TargetPath}", targetPath);
    }

    /// <summary>
    /// Reclaims disk space from a collection by rewriting it without deleted records.
    /// The collection must be loaded before calling this method.
    /// </summary>
    public async Task VacuumAsync(string collectionName)
    {
        if (!_collectionsByName.TryGetValue(collectionName, out var obj) || obj is not IVacuumable vacuumable)
            throw new InvalidOperationException($"Collection '{collectionName}' not found or not loaded");

        await vacuumable.VacuumAsync();
    }

    /// <summary>
    /// Create a checkpoint in WAL
    /// </summary>
    public Task<long> CreateCheckpointAsync()
    {
        if (_walManager == null)
        {
            _logger.Warning("WAL is not enabled, cannot create checkpoint");
            return Task.FromResult(-1L);
        }

        lock (_checkpointSync)
        {
            if (_activeCheckpointTask is { IsCompleted: false })
                return _activeCheckpointTask;

            _activeCheckpointTask = CreateCheckpointInternalAsync();
            return _activeCheckpointTask;
        }
    }

    private async Task<long> CreateCheckpointInternalAsync()
    {
        _logger.Information("Creating checkpoint");

        // Flush all pending changes
        await FlushAsync();

        // Create checkpoint in WAL
        var checkpointLsn = await _walManager!.CreateCheckpointAsync();

        _logger.Information("Checkpoint created at LSN {LSN}", checkpointLsn);

        return checkpointLsn;
    }

    private static ISerializer<TType> CreateSerializer<TType>() where TType : IComparable<TType>
        => SerializerRegistry.GetSerializer<TType>();

    private INodeFactory<TK, object> CreateNodeFactory<TK>(
        ISerializer<TK> keySerializer, 
        ISerializer<Pointer> valueSerializer,
        int degree) where TK : IComparable<TK>
    {
        var factory = new BPlusTreeNodeFactory<TK, Pointer>(keySerializer, valueSerializer, degree);
        // allowCreateInternalNode = false: This is for range query sessions, only deserializes nodes
        return new ObjectNodeFactoryAdapter<TK, Pointer>(factory, allowCreateInternalNode: false);
    }

    private Task<IForeignKeyLookup?> ResolveCollectionAsync(string collectionName)
    {
        if (_collectionsByName.TryGetValue(collectionName, out var collection) &&
            collection is IForeignKeyLookup fkLookup)
            return Task.FromResult<IForeignKeyLookup?>(fkLookup);

        return Task.FromResult<IForeignKeyLookup?>(null);
    }
}
