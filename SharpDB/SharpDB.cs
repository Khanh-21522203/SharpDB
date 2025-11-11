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
    private readonly string _basePath;
    private readonly Dictionary<int, object> _collections = new();
    private readonly EngineConfig _config;
    private readonly IDatabaseHeaderManager _dbHeaderManager;
    private readonly IDatabaseStorageManager _dbStorage;
    private readonly IFileHandlerPool _filePool;
    private readonly IIndexStorageManager _indexStorage;
    private readonly ILogger _logger = Log.ForContext<SharpDB>();
    private readonly ITransactionManager _transactionManager;
    private readonly WALManager? _walManager;

    public SharpDB(string basePath, EngineConfig? config = null)
    {
        _basePath = basePath;
        _config = config ?? EngineConfig.Default;

        Directory.CreateDirectory(basePath);

        // Initialize WAL Manager first (if WAL is enabled)
        if (_config.EnableWAL)
        {
            _walManager = new WALManager(basePath, _config.WALMaxFileSize);
            
            // Perform recovery from WAL
            var recoveryTask = _walManager.RecoverAsync();
            recoveryTask.Wait(); // Synchronously wait for recovery to complete
            var recoveryResult = recoveryTask.Result;
            
            _logger.Information("WAL recovery completed. Committed: {Committed}, Aborted: {Aborted}, Rolled back: {RolledBack}",
                recoveryResult.CommittedTransactions, 
                recoveryResult.AbortedTransactions,
                recoveryResult.UnfinishedTransactions);
        }

        // Initialize components
        _filePool = new FileHandlerPool(_logger, _config);
        IPageManager pageManager = new PageManager(basePath, _filePool, _config.PageSize, _config.Cache.PageCacheSize);
        _dbHeaderManager = new DatabaseHeaderManager(basePath);
        _dbStorage = new DiskPageDatabaseStorageManager(pageManager, _logger, _dbHeaderManager, _config);
        _indexStorage = new DiskPageFileIndexStorageManager(basePath, _logger, _filePool);

        var lockManager = new LockManager();
        var versionManager = new VersionManager(_dbStorage);
        var serializer = new JsonObjectSerializer();

        _transactionManager = new TransactionManager(lockManager, versionManager, serializer, _walManager);
    }

    public void Dispose()
    {
        FlushAsync().Wait();

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
        Func<T, TKey> keyExtractor)
        where T : class
        where TKey : IComparable<TKey>
    {
        var collectionId = await _dbHeaderManager.CreateCollectionAsync(name, schema);

        // Create primary index with correct TKey type
        var keySerializer = CreateSerializer<TKey>();
        var valueSerializer = new PointerSerializer();

        var primaryIndex = new BPlusTreeIndexManager<TKey, Pointer>(
            _indexStorage,
            collectionId,
            _config.BTreeDegree);

        // Create index session for range queries
        var nodeFactory = CreateNodeFactory<TKey>(keySerializer, valueSerializer, _config.BTreeDegree);
        var indexSession = new BufferedIndexIOSession<TKey>(
            _indexStorage,
            nodeFactory,
            collectionId);

        var dataSession = new BufferedDataIOSession(_dbStorage, _config);

        var collection = new CollectionManager<T, TKey>(
            collectionId,
            schema,
            dataSession,
            _indexStorage,
            primaryIndex,
            indexSession,
            keyExtractor
        );

        _collections[collectionId] = collection;

        return collection;
    }

    public async Task<CollectionManager<T, TKey>> GetCollectionAsync<T, TKey>(
        string name,
        Func<T, TKey> keyExtractor)
        where T : class
        where TKey : IComparable<TKey>
    {
        var collections = await _dbHeaderManager.GetCollectionsAsync();
        var collectionInfo = collections.FirstOrDefault(c => c.Name == name);

        if (collectionInfo == null)
            throw new InvalidOperationException($"Collection '{name}' not found");

        if (_collections.TryGetValue(collectionInfo.CollectionId, out var existing))
            return (CollectionManager<T, TKey>)existing;

        var schema = await _dbHeaderManager.GetSchemaAsync(collectionInfo.CollectionId);
        return await CreateCollectionAsync<T, TKey>(name, schema!, keyExtractor);
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
        await _filePool.FlushAllAsync();
        _walManager?.Flush();
    }

    /// <summary>
    /// Create a checkpoint in WAL
    /// </summary>
    public async Task<long> CreateCheckpointAsync()
    {
        if (_walManager == null)
        {
            _logger.Warning("WAL is not enabled, cannot create checkpoint");
            return -1;
        }

        _logger.Information("Creating checkpoint");
        
        // Flush all pending changes
        await FlushAsync();
        
        // Create checkpoint in WAL
        var checkpointLsn = await _walManager.CreateCheckpointAsync();
        
        _logger.Information("Checkpoint created at LSN {LSN}", checkpointLsn);
        
        return checkpointLsn;
    }

    private ISerializer<TType> CreateSerializer<TType>() where TType : IComparable<TType>
    {
        var type = typeof(TType);
        if (type == typeof(long)) return (ISerializer<TType>)new LongSerializer();
        if (type == typeof(int)) return (ISerializer<TType>)new IntSerializer();
        if (type == typeof(string)) return (ISerializer<TType>)new StringSerializer(255);
        if (type == typeof(DateTime)) return (ISerializer<TType>)new DateTimeSerializer();
        if (type == typeof(decimal)) return (ISerializer<TType>)new DecimalSerializer();
        if (type == typeof(Guid)) return (ISerializer<TType>)new GuidSerializer();
        throw new NotSupportedException($"Type {type} not supported as key type");
    }

    private INodeFactory<TK, object> CreateNodeFactory<TK>(
        ISerializer<TK> keySerializer, 
        ISerializer<Pointer> valueSerializer,
        int degree) where TK : IComparable<TK>
    {
        var factory = new BPlusTreeNodeFactory<TK, Pointer>(keySerializer, valueSerializer, degree);
        // allowCreateInternalNode = false: This is for range query sessions, only deserializes nodes
        return new ObjectNodeFactoryAdapter<TK, Pointer>(factory, allowCreateInternalNode: false);
    }
}