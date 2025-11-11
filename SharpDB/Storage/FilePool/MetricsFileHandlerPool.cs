using Serilog;
using SharpDB.Configuration;

namespace SharpDB.Storage.FilePool;

public class MetricsFileHandlerPool(
    long totalRequests,
    long cacheHits,
    long cacheMisses,
    ILogger logger,
    EngineConfig config)
    : FileHandlerPool(logger, config)
{
    private long _cacheHits = cacheHits;
    private long _cacheMisses = cacheMisses;
    private long _totalRequests = totalRequests;

    public double CacheHitRate => _totalRequests > 0
        ? (double)_cacheHits / _totalRequests
        : 0;

    public new async Task<FileStream> GetHandleAsync(int collectionId, string filePath)
    {
        Interlocked.Increment(ref _totalRequests);

        var wasInCache = _handles.ContainsKey(filePath);
        var handle = await base.GetHandleAsync(collectionId, filePath);

        if (wasInCache)
            Interlocked.Increment(ref _cacheHits);
        else
            Interlocked.Increment(ref _cacheMisses);

        return handle;
    }

    public FileHandlerPoolStatistics GetStatistics()
    {
        return new FileHandlerPoolStatistics
        {
            TotalRequests = _totalRequests,
            CacheHits = _cacheHits,
            CacheMisses = _cacheMisses,
            CacheHitRate = CacheHitRate,
            ActiveHandles = _handles.Count,
            MaxHandles = _maxConcurrentHandles
        };
    }
}

public class FileHandlerPoolStatistics
{
    public long TotalRequests { get; init; }
    public long CacheHits { get; init; }
    public long CacheMisses { get; init; }
    public double CacheHitRate { get; init; }
    public int ActiveHandles { get; init; }
    public int MaxHandles { get; init; }
}