┌────────────────────────────────── SHARPDB ARCHITECTURE ──────────────────────────────────┐
│                                                                                            │
│ ┌─────────────────────── CORE ABSTRACTIONS (Interfaces) ────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ ILockManager        │  │ IVersionManager  │  │ ITransactionManager   │     │        │
│ │  │ • AcquireLockAsync()│  │ • ReadAsync()    │  │ • BeginTransactionAsync│     │        │
│ │  │ • ReleaseLockAsync()│  │ • WriteAsync()   │  │ • CommitAsync()       │     │        │
│ │  │ • IsLockedAsync()   │  │ • CommitAsync()  │  │ • RollbackAsync()     │     │        │
│ │  └─────────────────────┘  │ • AbortAsync()   │  └───────────────────────┘     │        │
│ │                            └──────────────────┘                                │        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ IPageManager        │  │ IDatabaseStorage │  │ IIndexStorageManager  │     │        │
│ │  │ • AllocatePageAsync │  │   Manager        │  │ • CreateIndexAsync()  │     │        │
│ │  │ • LoadPageAsync()   │  │ • AllocatePointer│  │ • OpenIndexAsync()    │     │        │
│ │  │ • WritePageAsync()  │  │ • ReadAsync()    │  │ • DeleteIndexAsync()  │     │        │
│ │  │ • FreePageAsync()   │  │ • WriteAsync()   │  └───────────────────────┘     │        │
│ │  └─────────────────────┘  │ • DeleteAsync()  │                                │        │
│ │                            └──────────────────┘                                │        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ ITreeIndexManager   │  │ ISerializer<T>   │  │ IObjectSerializer     │     │        │
│ │  │ • SearchAsync()     │  │ • Serialize()    │  │ • Serialize<T>()      │     │        │
│ │  │ • InsertAsync()     │  │ • Deserialize()  │  │ • Deserialize<T>()    │     │        │
│ │  │ • DeleteAsync()     │  │ • GetSize()      │  └───────────────────────┘     │        │
│ │  │ • UpdateAsync()     │  └──────────────────┘                                │        │
│ │  └─────────────────────┘                                                       │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── ENGINE & TRANSACTION LAYER ────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ TransactionManager  │  │ Transaction      │  │ VersionManager        │     │        │
│ │  │ • _lockManager      │  │ • TransactionId  │  │ • _dbStorage          │     │        │
│ │  │ • _versionManager   │  │ • StartTimestamp │  │ • ReadAsync()         │     │        │
│ │  │ • _walManager       │  │ • IsolationLevel │  │ • WriteAsync()        │     │        │
│ │  │ • BeginTransaction()│  │ • ReadAsync<T>() │  │ • CommitAsync()       │     │        │
│ │  │ • CommitAsync()     │  │ • WriteAsync<T>()│  │ // implements         │     │        │
│ │  │ • RollbackAsync()   │  │ // uses WAL      │  │ // IVersionManager    │     │        │
│ │  │ // implements       │  └──────────────────┘  └───────────────────────┘     │        │
│ │  │ // ITransactionMgr  │                                                       │        │
│ │  └─────────────────────┘                                                       │        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ LockManager         │  │ DeadlockDetector │  │ CollectionManager<T>  │     │        │
│ │  │ • _locks: Dict      │  │ • _waitGraph     │  │ • _primaryIndex       │     │        │
│ │  │ • _txnLocks: Dict   │  │ • AddWait()      │  │ • _secondaryIndexes   │     │        │
│ │  │ • _deadlockDetector │  │ • RemoveWait()   │  │ • InsertAsync()       │     │        │
│ │  │ • AcquireLockAsync()│  │ • DetectDeadlock │  │ • SelectAsync()       │     │        │
│ │  │ • ReleaseLockAsync()│  └──────────────────┘  │ • UpdateAsync()       │     │        │
│ │  │ // implements       │                        │ • DeleteAsync()       │     │        │
│ │  │ // ILockManager     │                        │ • ScanAsync()         │     │        │
│ │  └─────────────────────┘                        └───────────────────────┘     │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── STORAGE LAYER ─────────────────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ PageManager         │  │ Page             │  │ FileHandlerPool       │     │        │
│ │  │ • _pageCache:       │  │ • PageNumber     │  │ • _pool: Dict         │     │        │
│ │  │   LruCache<Page>    │  │ • Data: byte[]   │  │ • _maxHandles         │     │        │
│ │  │ • _activePages      │  │ • IsModified     │  │ • GetHandleAsync()    │     │        │
│ │  │ • AllocatePageAsync │  │ • ClearModified()│  │ • CloseAsync()        │     │        │
│ │  │ • LoadPageAsync()   │  │ • Dispose()      │  │ • FlushAllAsync()     │     │        │
│ │  │ • WritePageAsync()  │  └──────────────────┘  └───────────────────────┘     │        │
│ │  │ • FreePageAsync()   │                                                       │        │
│ │  │ // uses LruCache    │  ┌──────────────────────────────────────────────┐    │        │
│ │  │ // implements       │  │ DiskPageDatabaseStorageManager                │    │        │
│ │  │ // IPageManager     │  │ • _pageManager: IPageManager                 │    │        │
│ │  └─────────────────────┘  │ • _dbHeaderManager: IDatabaseHeaderManager   │    │        │
│ │                            │ • AllocatePointerAsync()                     │    │        │
│ │                            │ • ReadAsync() / WriteAsync()                 │    │        │
│ │                            │ // implements IDatabaseStorageManager        │    │        │
│ │                            └──────────────────────────────────────────────┘    │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── WAL (Write-Ahead Logging) ─────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ WALManager          │  │ LogRecord (base) │  │ UpdateLogRecord       │     │        │
│ │  │ • _logDirectory     │  │ • LSN            │  │ • CollectionId        │     │        │
│ │  │ • _currentLogFile   │  │ • TransactionId  │  │ • PagePointer         │     │        │
│ │  │ • _pendingRecords   │  │ • Type (enum)    │  │ • BeforeImage: byte[] │     │        │
│ │  │ • WriteLogRecord()  │  │ • PrevLSN        │  │ • AfterImage: byte[]  │     │        │
│ │  │ • RecoverAsync()    │  │ • Serialize()    │  │ // extends LogRecord  │     │        │
│ │  │ • CreateCheckpoint()│  │ • Deserialize()  │  └───────────────────────┘     │        │
│ │  │ • LogTransBegin()   │  └──────────────────┘                                │        │
│ │  │ • LogTransCommit()  │                                                       │        │
│ │  │ • LogTransAbort()   │  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ • Flush()           │  │ BeginLogRecord   │  │ CheckpointLogRecord   │     │        │
│ │  └─────────────────────┘  │ // extends       │  │ • ActiveTransactions  │     │        │
│ │                            │ // LogRecord     │  │ • TransactionLastLSN  │     │        │
│ │                            └──────────────────┘  │ // extends LogRecord  │     │        │
│ │                                                  └───────────────────────┘     │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── INDEX & B+ TREE LAYER ─────────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ BPlusTreeIndexMgr   │  │ LeafNode<K,V>    │  │ InternalNode<K>       │     │        │
│ │  │ • _root: Node       │  │ • Keys: List<K>  │  │ • Keys: List<K>       │     │        │
│ │  │ • _degree: int      │  │ • Values: List<V>│  │ • Children: List<ptr> │     │        │
│ │  │ • SearchAsync()     │  │ • Next: pointer  │  │ • IsLeaf = false      │     │        │
│ │  │ • InsertAsync()     │  │ • IsLeaf = true  │  │ • FindChild()         │     │        │
│ │  │ • DeleteAsync()     │  │ • Insert()       │  │ • Split()             │     │        │
│ │  │ • SplitNode()       │  │ • Delete()       │  └───────────────────────┘     │        │
│ │  │ // implements       │  │ • Split()        │                                │        │
│ │  │ // ITreeIndexMgr    │  └──────────────────┘                                │        │
│ │  └─────────────────────┘                                                       │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── DATA STRUCTURES ────────────────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ LruCache<K,V>       │  │ Pointer (struct) │  │ KeyValue<K,V> (struct)│     │        │
│ │  │ • _cache: Dict      │  │ • PageNumber     │  │ • Key: K              │     │        │
│ │  │ • _lruList: List    │  │ • RecordIndex    │  │ • Value: V            │     │        │
│ │  │ • _capacity         │  │ • IsEmpty()      │  └───────────────────────┘     │        │
│ │  │ • TryGet()          │  │ • Empty()        │                                │        │
│ │  │ • Put()             │  │ • ToString()     │  ┌───────────────────────┐     │        │
│ │  │ • Remove()          │  │ • FromBytes()    │  │ BinaryList            │     │        │
│ │  │ • Clear()           │  │ • FillBytes()    │  │ • _data: byte[]       │     │        │
│ │  └─────────────────────┘  └──────────────────┘  │ • Add() / Get()       │     │        │
│ │                                                  │ • Count / Capacity    │     │        │
│ │                                                  └───────────────────────┘     │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── SERIALIZATION LAYER ────────────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ JsonObjectSerializer│  │ IntSerializer    │  │ StringSerializer      │     │        │
│ │  │ • Serialize<T>()    │  │ • Serialize()    │  │ • _maxLength          │     │        │
│ │  │ • Deserialize<T>()  │  │ • Deserialize()  │  │ • Serialize()         │     │        │
│ │  │ // implements       │  │ • GetSize()      │  │ • Deserialize()       │     │        │
│ │  │ // IObjectSerializer│  │ // implements    │  │ // implements         │     │        │
│ │  └─────────────────────┘  │ // ISerializer   │  │ // ISerializer<string>│     │        │
│ │                            └──────────────────┘  └───────────────────────┘     │        │
│ │                                                                                 │        │
│ │  ┌─────────────────────┐  ┌──────────────────┐  ┌───────────────────────┐     │        │
│ │  │ DateTimeSerializer  │  │ DecimalSerializer│  │ LongSerializer        │     │        │
│ │  │ • Serialize()       │  │ • Serialize()    │  │ • Serialize()         │     │        │
│ │  │ • Deserialize()     │  │ • Deserialize()  │  │ • Deserialize()       │     │        │
│ │  │ // implements       │  │ // implements    │  │ // implements         │     │        │
│ │  │ // ISerializer      │  │ // ISerializer   │  │ // ISerializer<long>  │     │        │
│ │  └─────────────────────┘  └──────────────────┘  └───────────────────────┘     │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
│ ┌─────────────────────── MAIN & CONFIGURATION ──────────────────────────────────┐        │
│ │                                                                                 │        │
│ │  ┌─────────────────────────────────────────────┐  ┌───────────────────────┐   │        │
│ │  │ SharpDB (Main Class)                        │  │ EngineConfig          │   │        │
│ │  │ • _basePath: string                         │  │ • PageSize            │   │        │
│ │  │ • _collections: Dictionary<int,object>      │  │ • MaxFileHandles      │   │        │
│ │  │ • _config: EngineConfig                     │  │ • BTreeDegree         │   │        │
│ │  │ • _filePool: IFileHandlerPool               │  │ • EnableWAL           │   │        │
│ │  │ • _pageManager: IPageManager                │  │ • WALMaxFileSize      │   │        │
│ │  │ • _dbStorage: IDatabaseStorageManager       │  │ • WALCheckpointInterval│  │        │
│ │  │ • _indexStorage: IIndexStorageManager       │  │ • Storage: StorageConfig│ │        │
│ │  │ • _transactionManager: ITransactionManager  │  │ • Index: IndexConfig  │   │        │
│ │  │ • _walManager: WALManager                   │  │ • Cache: CacheConfig  │   │        │
│ │  │ • CreateCollectionAsync<T,TKey>()           │  └───────────────────────┘   │        │
│ │  │ • GetCollectionAsync<T,TKey>()              │                               │        │
│ │  │ • BeginTransactionAsync()                   │  ┌───────────────────────┐   │        │
│ │  │ • CommitTransactionAsync()                  │  │ Schema                │   │        │
│ │  │ • RollbackTransactionAsync()                │  │ • Fields: List<Field> │   │        │
│ │  │ • CreateCheckpointAsync()                   │  │ • Validate()          │   │        │
│ │  │ • FlushAsync()                              │  └───────────────────────┘   │        │
│ │  │ • Dispose()                                 │                               │        │
│ │  └─────────────────────────────────────────────┘                               │        │
│ └─────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────── RELATIONSHIPS ────────────────────────────────────────────┐
│                                                                                              │
│  SharpDB ──owns──> TransactionManager ──uses──> WALManager ──writes──> LogRecord            │
│     │                   │                           │                                        │
│     │                   │                           └──manages──> LogFile (wal_*.log)        │
│     │                   │                                                                    │
│     │                   ├──owns──> LockManager ──uses──> DeadlockDetector                    │
│     │                   │              │                                                     │
│     │                   │              └──manages──> LockEntry                               │
│     │                   │                                                                    │
│     │                   └──owns──> VersionManager ──uses──> DatabaseStorageManager           │
│     │                                                                                        │
│     ├──owns──> PageManager ──uses──> LruCache<string, Page>                                 │
│     │              │                      │                                                 │
│     │              │                      └──caches──> Page                                 │
│     │              │                                                                         │
│     │              ├──uses──> FileHandlerPool ──manages──> PooledFileHandle                 │
│     │              │                                                                         │
│     │              └──manages──> Page ──contains──> byte[] (page data)                      │
│     │                                                                                        │
│     ├──owns──> DatabaseStorageManager ──uses──> PageManager                                 │
│     │              │                                                                         │
│     │              └──uses──> DatabaseHeaderManager                                         │
│     │                                                                                        │
│     ├──owns──> IndexStorageManager ──manages──> BPlusTreeIndexManager                       │
│     │                                     │                                                 │
│     │                                     ├──owns──> LeafNode<K,V>                          │
│     │                                     │              │                                  │
│     │                                     │              └──contains──> Keys & Values       │
│     │                                     │                                                 │
│     │                                     └──owns──> InternalNode<K>                        │
│     │                                                    │                                  │
│     │                                                    └──contains──> Keys & Pointers     │
│     │                                                                                        │
│     ├──manages──> CollectionManager<T,TKey>                                                 │
│     │                   │                                                                   │
│     │                   ├──uses──> BPlusTreeIndexManager (primary index)                    │
│     │                   │                                                                   │
│     │                   ├──uses──> List<SecondaryIndexWrapper> (secondary indexes)          │
│     │                   │                                                                   │
│     │                   └──uses──> Serializer<TKey> & ObjectSerializer                      │
│     │                                                                                        │
│     └──creates──> Transaction ──uses──> LockManager                                         │
                │                  │                                                          │
                │                  ├──uses──> VersionManager                                  │
                │                  │                                                          │
                │                  └──logs_to──> WALManager ──creates──> UpdateLogRecord      │
                │                                                                              │
                └──performs──> CRUD Operations:                                               │
                         • InsertAsync() ──calls──> CollectionManager.InsertAsync()           │
                         • SelectAsync() ──calls──> CollectionManager.SelectAsync()           │
                         • UpdateAsync() ──calls──> CollectionManager.UpdateAsync()           │
                         • DeleteAsync() ──calls──> CollectionManager.DeleteAsync()           │
                                                                                               │
│  WAL Recovery Flow:                                                                         │
│     WALManager.RecoverAsync() ──phases──> 1. Analysis (identify transactions)               │
│                                          ──> 2. REDO (apply committed operations)           │
│                                          ──> 3. UNDO (rollback uncommitted)                 │
│                                                                                              │
│  Serialization Pipeline:                                                                    │
│     CollectionManager ──uses──> JsonObjectSerializer ──for──> Complex Objects               │
│                      ──uses──> IntSerializer / LongSerializer ──for──> Keys                 │
│                      ──uses──> DateTimeSerializer / DecimalSerializer ──for──> Values       │
│                                                                                              │
│  Caching Strategy:                                                                          │
│     PageManager.LoadPageAsync() ──checks──> LruCache                                        │
│                                      │                                                      │
│                                      ├──hit──> Return cached Page                           │
│                                      │                                                      │
│                                      └──miss──> Load from disk ──> Add to LruCache          │
│                                                                                              │
│  Concurrency Control:                                                                       │
│     Transaction.WriteAsync() ──acquires──> Exclusive Lock (via LockManager)                 │
│                                    │                                                        │
│                                    └──checks──> DeadlockDetector                            │
│                                             │                                               │
│                                             └──if_deadlock──> Abort victim transaction      │
│                                                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────── API FLOWS ──────────────────────────────────────────────┐
│                                                                                              │
│  ┌─── 1. CREATE COLLECTION FLOW ────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: db.CreateCollectionAsync<Person, long>("persons", schema, p => p.Id)          │  │
│  │    │                                                                                  │  │
│  │    └──> SharpDB.CreateCollectionAsync()                                              │  │
│  │           │                                                                           │  │
│  │           ├──> Schema.Validate() // Check field definitions                          │  │
│  │           │                                                                           │  │
│  │           ├──> _dbHeaderManager.RegisterCollection() // Get collection ID            │  │
│  │           │                                                                           │  │
│  │           ├──> CreateSerializer<TKey>() // Create key serializer                     │  │
│  │           │                                                                           │  │
│  │           ├──> _indexStorage.CreateIndexAsync() // Create B+ Tree for primary key    │  │
│  │           │      │                                                                    │  │
│  │           │      └──> BPlusTreeIndexManager.Initialize()                             │  │
│  │           │                                                                           │  │
│  │           ├──> Create secondary indexes (if any indexed fields)                      │  │
│  │           │                                                                           │  │
│  │           └──> new CollectionManager<T, TKey>() // Return collection                 │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 2. INSERT FLOW ────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: collection.InsertAsync(person)                                                 │  │
│  │    │                                                                                  │  │
│  │    └──> CollectionManager.InsertAsync()                                              │  │
│  │           │                                                                           │  │
│  │           ├──> Extract key from entity: _keyExtractor(entity) → primaryKey           │  │
│  │           │                                                                           │  │
│  │           ├──> _objectSerializer.Serialize(entity) → byte[]                          │  │
│  │           │                                                                           │  │
│  │           ├──> _dbStorage.AllocatePointerAsync() → Pointer (PageNum, RecordIndex)    │  │
│  │           │      │                                                                    │  │
│  │           │      └──> PageManager.AllocatePageAsync() // If needed                   │  │
│  │           │                                                                           │  │
│  │           ├──> _dbStorage.WriteAsync(pointer, data) // Write to storage              │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> PageManager.LoadPageAsync(pageNum)                             │  │
│  │           │      │      │                                                             │  │
│  │           │      │      ├──> LruCache.TryGet() // Check cache                        │  │
│  │           │      │      └──> Load from disk if cache miss                            │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> Page.Write(recordIndex, data) // Write to page                 │  │
│  │           │      │                                                                    │  │
│  │           │      └──> PageManager.WritePageAsync() // Flush to disk                  │  │
│  │           │                                                                           │  │
│  │           ├──> _primaryIndex.InsertAsync(primaryKey, pointer) // Index entry         │  │
│  │           │      │                                                                    │  │
│  │           │      └──> BPlusTreeIndexManager.InsertAsync()                            │  │
│  │           │             │                                                             │  │
│  │           │             ├──> Navigate tree to find insertion point                   │  │
│  │           │             ├──> LeafNode.Insert(key, value)                             │  │
│  │           │             └──> Split node if full, propagate upward                    │  │
│  │           │                                                                           │  │
│  │           └──> Update secondary indexes (if any)                                     │  │
│  │                  │                                                                    │  │
│  │                  └──> For each secondary index: InsertAsync(indexKey, pointer)       │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 3. SELECT/READ FLOW ──────────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: person = collection.SelectAsync(personId)                                      │  │
│  │    │                                                                                  │  │
│  │    └──> CollectionManager.SelectAsync(key)                                           │  │
│  │           │                                                                           │  │
│  │           ├──> _primaryIndex.SearchAsync(key) → Pointer                              │  │
│  │           │      │                                                                    │  │
│  │           │      └──> BPlusTreeIndexManager.SearchAsync()                            │  │
│  │           │             │                                                             │  │
│  │           │             ├──> Navigate tree from root to leaf                         │  │
│  │           │             ├──> InternalNode.FindChild() // Binary search               │  │
│  │           │             ├──> Load child nodes recursively                            │  │
│  │           │             └──> LeafNode.Search(key) → value (Pointer)                  │  │
│  │           │                                                                           │  │
│  │           ├──> _dbStorage.ReadAsync(pointer) → byte[]                                │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> PageManager.LoadPageAsync(pointer.PageNumber)                  │  │
│  │           │      │      │                                                             │  │
│  │           │      │      ├──> LruCache.TryGet() // Hit: return cached page            │  │
│  │           │      │      └──> Miss: Load from disk → LruCache.Put()                   │  │
│  │           │      │                                                                    │  │
│  │           │      └──> Page.Read(pointer.RecordIndex) → byte[]                        │  │
│  │           │                                                                           │  │
│  │           └──> _objectSerializer.Deserialize<T>(data) → entity                       │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 4. UPDATE FLOW ────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: collection.UpdateAsync(updatedPerson)                                          │  │
│  │    │                                                                                  │  │
│  │    └──> CollectionManager.UpdateAsync(entity)                                        │  │
│  │           │                                                                           │  │
│  │           ├──> Extract key: _keyExtractor(entity) → primaryKey                       │  │
│  │           │                                                                           │  │
│  │           ├──> _primaryIndex.SearchAsync(key) → oldPointer                           │  │
│  │           │                                                                           │  │
│  │           ├──> _dbStorage.ReadAsync(oldPointer) → oldData (for WAL before-image)     │  │
│  │           │                                                                           │  │
│  │           ├──> _objectSerializer.Serialize(entity) → newData                         │  │
│  │           │                                                                           │  │
│  │           ├──> If (newData.Length <= oldData.Length): // In-place update             │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> _dbStorage.WriteAsync(oldPointer, newData)                     │  │
│  │           │      │                                                                    │  │
│  │           │      └──> WAL: LogUpdate(txnId, collectionId, pointer, oldData, newData) │  │
│  │           │                                                                           │  │
│  │           └──> Else: // Need reallocation                                            │  │
│  │                  │                                                                    │  │
│  │                  ├──> _dbStorage.DeleteAsync(oldPointer)                             │  │
│  │                  ├──> _dbStorage.AllocatePointerAsync() → newPointer                 │  │
│  │                  ├──> _dbStorage.WriteAsync(newPointer, newData)                     │  │
│  │                  ├──> _primaryIndex.UpdateAsync(key, oldPtr, newPtr)                 │  │
│  │                  └──> Update all secondary indexes                                   │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 5. DELETE FLOW ────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: deleted = collection.DeleteAsync(personId)                                     │  │
│  │    │                                                                                  │  │
│  │    └──> CollectionManager.DeleteAsync(key)                                           │  │
│  │           │                                                                           │  │
│  │           ├──> _primaryIndex.SearchAsync(key) → pointer                              │  │
│  │           │                                                                           │  │
│  │           ├──> _dbStorage.DeleteAsync(pointer) // Mark as deleted                    │  │
│  │           │      │                                                                    │  │
│  │           │      └──> PageManager.FreePageAsync(pointer.PageNumber)                  │  │
│  │           │             │                                                             │  │
│  │           │             └──> LruCache.Remove(cacheKey) // Invalidate cache           │  │
│  │           │                                                                           │  │
│  │           ├──> _primaryIndex.DeleteAsync(key) // Remove from index                   │  │
│  │           │      │                                                                    │  │
│  │           │      └──> BPlusTreeIndexManager.DeleteAsync()                            │  │
│  │           │             │                                                             │  │
│  │           │             ├──> Navigate to leaf containing key                         │  │
│  │           │             ├──> LeafNode.Delete(key)                                    │  │
│  │           │             └──> Rebalance tree if needed (merge/redistribute)           │  │
│  │           │                                                                           │  │
│  │           └──> Delete from all secondary indexes                                     │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 6. TRANSACTION FLOW ──────────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: var txn = db.BeginTransactionAsync(IsolationLevel.ReadCommitted)              │  │
│  │    │                                                                                  │  │
│  │    └──> TransactionManager.BeginTransactionAsync()                                   │  │
│  │           │                                                                           │  │
│  │           ├──> Assign new TransactionId (atomic increment)                           │  │
│  │           ├──> Assign StartTimestamp (atomic increment)                              │  │
│  │           │                                                                           │  │
│  │           ├──> WALManager.LogTransactionBegin(txnId)                                 │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> Create BeginLogRecord with LSN                                 │  │
│  │           │      ├──> Add to pending records buffer                                  │  │
│  │           │      └──> Auto-flush after timeout (100ms) or on commit                  │  │
│  │           │                                                                           │  │
│  │           └──> new Transaction(txnId, timestamp, level, lockMgr, versionMgr, wal)    │  │
│  │                                                                                        │  │
│  │  User performs operations within transaction...                                       │  │
│  │    │                                                                                  │  │
│  │    ├──> Transaction.WriteAsync(pointer, data)                                        │  │
│  │    │      │                                                                           │  │
│  │    │      ├──> LockManager.AcquireLockAsync(resourceId, txnId, EXCLUSIVE)            │  │
│  │    │      │      │                                                                    │  │
│  │    │      │      ├──> Check DeadlockDetector.DetectDeadlock()                        │  │
│  │    │      │      │      └──> If deadlock: abort victim transaction                   │  │
│  │    │      │      │                                                                    │  │
│  │    │      │      └──> LockEntry.AcquireExclusiveAsync()                              │  │
│  │    │      │                                                                           │  │
│  │    │      ├──> Get before-image: VersionManager.ReadAsync(pointer)                   │  │
│  │    │      │                                                                           │  │
│  │    │      ├──> VersionManager.WriteAsync(pointer, data, timestamp, txnId)            │  │
│  │    │      │                                                                           │  │
│  │    │      └──> WALManager.LogUpdate(txnId, collectionId, ptr, before, after)         │  │
│  │    │                                                                                  │  │
│  │    └──> Transaction.ReadAsync(pointer)                                               │  │
│  │           │                                                                           │  │
│  │           ├──> LockManager.AcquireLockAsync(resourceId, txnId, SHARED)               │  │
│  │           │                                                                           │  │
│  │           └──> VersionManager.ReadAsync(pointer, startTimestamp) // MVCC             │  │
│  │                                                                                        │  │
│  │  User: db.CommitTransactionAsync(txn)                                                 │  │
│  │    │                                                                                  │  │
│  │    └──> TransactionManager.CommitAsync()                                             │  │
│  │           │                                                                           │  │
│  │           ├──> WALManager.LogTransactionCommit(txnId)                                │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> Create CommitLogRecord with LSN                                │  │
│  │           │      ├──> Add to pending buffer                                          │  │
│  │           │      └──> WALManager.Flush() // Force flush for durability               │  │
│  │           │                                                                           │  │
│  │           ├──> VersionManager.CommitAsync(txnId, commitTimestamp)                    │  │
│  │           │      └──> Make all changes visible to other transactions                 │  │
│  │           │                                                                           │  │
│  │           └──> LockManager.ReleaseAllLocksAsync(txnId)                               │  │
│  │                  └──> Notify waiting transactions                                    │  │
│  │                                                                                        │  │
│  │  User: db.RollbackTransactionAsync(txn)                                               │  │
│  │    │                                                                                  │  │
│  │    └──> TransactionManager.RollbackAsync()                                           │  │
│  │           │                                                                           │  │
│  │           ├──> WALManager.LogTransactionAbort(txnId)                                 │  │
│  │           ├──> VersionManager.AbortAsync(txnId) // Undo all changes                  │  │
│  │           └──> LockManager.ReleaseAllLocksAsync(txnId)                               │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 7. CHECKPOINT FLOW ────────────────────────────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  User: checkpointLSN = db.CreateCheckpointAsync()                                     │  │
│  │    │                                                                                  │  │
│  │    └──> SharpDB.CreateCheckpointAsync()                                              │  │
│  │           │                                                                           │  │
│  │           ├──> db.FlushAsync() // Flush all pending changes                          │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> DatabaseStorageManager.FlushAsync()                            │  │
│  │           │      │      └──> Write all dirty pages to disk                           │  │
│  │           │      │                                                                    │  │
│  │           │      ├──> FileHandlerPool.FlushAllAsync()                                │  │
│  │           │      │      └──> Sync all file handles                                   │  │
│  │           │      │                                                                    │  │
│  │           │      └──> WALManager.Flush()                                             │  │
│  │           │             └──> Flush all pending log records                           │  │
│  │           │                                                                           │  │
│  │           └──> WALManager.CreateCheckpointAsync()                                    │  │
│  │                  │                                                                    │  │
│  │                  ├──> Create CheckpointLogRecord (start)                             │  │
│  │                  │      ├──> Record all active transaction IDs                       │  │
│  │                  │      └──> Record last LSN for each transaction                    │  │
│  │                  │                                                                    │  │
│  │                  ├──> Write checkpoint start record → LSN                            │  │
│  │                  ├──> Flush()                                                        │  │
│  │                  ├──> Create CheckpointLogRecord (end)                               │  │
│  │                  ├──> Write checkpoint end record                                    │  │
│  │                  └──> Flush() // Ensure durability                                   │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
│  ┌─── 8. RECOVERY FLOW (On Database Startup) ───────────────────────────────────────────┐  │
│  │                                                                                        │  │
│  │  System: new SharpDB(path, config) // With EnableWAL = true                           │  │
│  │    │                                                                                  │  │
│  │    └──> SharpDB Constructor                                                          │  │
│  │           │                                                                           │  │
│  │           ├──> new WALManager(basePath)                                              │  │
│  │           │      │                                                                    │  │
│  │           │      └──> Find latest log files (wal_*.log)                              │  │
│  │           │                                                                           │  │
│  │           └──> WALManager.RecoverAsync()                                             │  │
│  │                  │                                                                    │  │
│  │                  ├──> PHASE 1: ANALYSIS                                              │  │
│  │                  │      │                                                             │  │
│  │                  │      ├──> Read all log files sequentially                         │  │
│  │                  │      ├──> Build transaction state map:                            │  │
│  │                  │      │      • committedTransactions: HashSet<TxnId>               │  │
│  │                  │      │      • abortedTransactions: HashSet<TxnId>                 │  │
│  │                  │      │      • activeTransactions: Dict<TxnId, List<LogRecord>>    │  │
│  │                  │      │                                                             │  │
│  │                  │      └──> Find last checkpoint (if any) to optimize recovery      │  │
│  │                  │                                                                    │  │
│  │                  ├──> PHASE 2: REDO                                                  │  │
│  │                  │      │                                                             │  │
│  │                  │      ├──> Replay all log records from committed transactions      │  │
│  │                  │      │                                                             │  │
│  │                  │      └──> For each UpdateLogRecord in committedTransactions:      │  │
│  │                  │             │                                                      │  │
│  │                  │             ├──> Load page from disk                              │  │
│  │                  │             ├──> Apply after-image to page                        │  │
│  │                  │             └──> Write page back to disk                          │  │
│  │                  │                                                                    │  │
│  │                  └──> PHASE 3: UNDO                                                  │  │
│  │                         │                                                             │  │
│  │                         ├──> For each transaction in activeTransactions:             │  │
│  │                         │      │                                                      │  │
│  │                         │      ├──> Process log records in REVERSE order             │  │
│  │                         │      │                                                      │  │
│  │                         │      ├──> For each UpdateLogRecord:                        │  │
│  │                         │      │      │                                               │  │
│  │                         │      │      ├──> Load page from disk                       │  │
│  │                         │      │      ├──> Apply before-image to page (undo)         │  │
│  │                         │      │      ├──> Write page back                           │  │
│  │                         │      │      │                                               │  │
│  │                         │      │      └──> Write CLR (Compensation Log Record)       │  │
│  │                         │      │                                                      │  │
│  │                         │      └──> Write AbortLogRecord for transaction             │  │
│  │                         │                                                             │  │
│  │                         └──> Recovery complete! Database is consistent                │  │
│  │                                                                                        │  │
│  └────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘