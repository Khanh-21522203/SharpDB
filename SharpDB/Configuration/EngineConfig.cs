using SharpDB.Core.Abstractions.Concurrency;

namespace SharpDB.Configuration;

public class EngineConfig
{
    public int PageSize { get; set; } = 4096;
    public int MaxFileHandles { get; set; } = 100;
    public int BTreeDegree { get; set; } = 128;
    public bool UseBufferedIO { get; set; } = true;
    public IsolationLevel DefaultIsolationLevel { get; set; } = IsolationLevel.ReadCommitted;
    
    // WAL Configuration
    public bool EnableWAL { get; set; } = true;
    public int WALMaxFileSize { get; set; } = 10 * 1024 * 1024; // 10MB default
    public int WALCheckpointInterval { get; set; } = 1000; // Checkpoint every 1000 transactions
    public bool WALAutoCheckpoint { get; set; } = true;

    public StorageConfig Storage { get; set; } = new();
    public IndexConfig Index { get; set; } = new();
    public CacheConfig Cache { get; set; } = new();

    public static EngineConfig Default => new();

    public static EngineConfig HighPerformance => new()
    {
        PageSize = 8192,
        MaxFileHandles = 500,
        BTreeDegree = 256,
        UseBufferedIO = true,
        Cache = new CacheConfig
        {
            PageCacheSize = 10000,
            IndexCacheSize = 5000
        }
    };

    public static EngineConfig LowMemory => new()
    {
        PageSize = 2048,
        MaxFileHandles = 50,
        BTreeDegree = 64,
        Cache = new CacheConfig
        {
            PageCacheSize = 100,
            IndexCacheSize = 50
        }
    };
}

public class StorageConfig
{
    public bool EnableCompression { get; set; } = false;
    public int CompressionThreshold { get; set; } = 512;
    public bool EnableChecksums { get; set; } = true;
}

public class IndexConfig
{
    public int MinDegree { get; set; } = 32;
    public int MaxDegree { get; set; } = 256;
    public bool AutoOptimizeDegree { get; set; } = true;
}

public class CacheConfig
{
    public int PageCacheSize { get; set; } = 1000;
    public int IndexCacheSize { get; set; } = 500;
    public bool EnableLRU { get; set; } = true;
}