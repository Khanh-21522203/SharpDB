using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;

namespace SharpDB.Storage.FilePool;

/// <summary>
/// Connection pool for file handles with concurrency control.
/// </summary>
public class FileHandlerPool(int maxConcurrentHandles = 100) : IFileHandlerPool
{
    private readonly ConcurrentDictionary<int, SemaphoreSlim> _locks = new();
    private readonly ConcurrentDictionary<int, FileStream> _handles = new();
    private readonly int _maxConcurrentHandles = maxConcurrentHandles;
    private readonly SemaphoreSlim _globalLimit = new(maxConcurrentHandles, maxConcurrentHandles);

    public async Task<FileStream> GetHandleAsync(int collectionId, string filePath)
    {
        await _globalLimit.WaitAsync();
        
        try
        {
            // Get or create per-collection lock
            var collectionLock = _locks.GetOrAdd(
                collectionId,
                _ => new SemaphoreSlim(1, 1)
            );
            
            await collectionLock.WaitAsync();
            
            try
            {
                // Check if handle already exists
                if (_handles.TryGetValue(collectionId, out var existingHandle))
                {
                    return existingHandle;
                }
                
                // Create new handle
                var newHandle = new FileStream(
                    filePath,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 8192,
                    useAsync: true
                );
                
                _handles[collectionId] = newHandle;
                return newHandle;
            }
            finally
            {
                collectionLock.Release();
            }
        }
        finally
        {
            _globalLimit.Release();
        }
    }

    public async Task ReleaseHandleAsync(int collectionId)
    {
        if (_handles.TryRemove(collectionId, out var handle))
        {
            await handle.FlushAsync();
            await handle.DisposeAsync();
        }
        
        if (_locks.TryRemove(collectionId, out var semaphore))
        {
            semaphore.Dispose();
        }
    }

    public async Task FlushAllAsync()
    {
        foreach (var handle in _handles.Values)
        {
            await handle.FlushAsync();
        }
    }
    
    public void Dispose()
    {
        FlushAllAsync().Wait();
        
        foreach (var handle in _handles.Values)
        {
            handle?.Dispose();
        }
        
        _handles.Clear();
        
        foreach (var semaphore in _locks.Values)
        {
            semaphore?.Dispose();
        }
        
        _locks.Clear();
        _globalLimit?.Dispose();
    }
}

/// <summary>
/// Pooled file handle wrapper with automatic return to pool.
/// </summary>
public class PooledFileHandle(FileStream handle, Action returnToPool) : IDisposable
{
    public FileStream Handle => handle;

    public void Dispose()
    {
        returnToPool?.Invoke();
    }
}