using System.Collections.Concurrent;
using Serilog;
using SharpDB.Core.Abstractions.Storage;

namespace SharpDB.Storage.FilePool;

/// <summary>
/// Connection pool for file handles with concurrency control.
/// Prevents file handle exhaustion and improves performance.
/// </summary>
public class FileHandlerPool : IFileHandlerPool
{
    private readonly ConcurrentDictionary<int, SemaphoreSlim> _locks = new();
    protected readonly ConcurrentDictionary<int, FileStream> _handles = new();
    protected readonly int _maxConcurrentHandles;
    private readonly SemaphoreSlim _globalLimit;
    private readonly ILogger _logger;
    private bool _disposed;
    
    public FileHandlerPool(ILogger logger, int maxConcurrentHandles = 100)
    {
        if (maxConcurrentHandles <= 0)
            throw new ArgumentException("Max handles must be positive", nameof(maxConcurrentHandles));
            
        _maxConcurrentHandles = maxConcurrentHandles;
        _globalLimit = new SemaphoreSlim(maxConcurrentHandles, maxConcurrentHandles);
        _logger = logger;
    }
    
    /// <summary>
    /// Get or create file handle with locking.
    /// </summary>
    public async Task<FileStream> GetHandleAsync(int collectionId, string filePath)
    {
        ThrowIfDisposed();
        
        if (string.IsNullOrEmpty(filePath))
            throw new ArgumentException("File path cannot be empty", nameof(filePath));
        
        // Wait for global limit
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
                    if (!existingHandle.CanRead || !existingHandle.CanWrite)
                    {
                        // Handle is closed or corrupted, remove it
                        _logger.Warning(
                            "Handle for collection {CollectionId} is corrupted, creating new one",
                            collectionId
                        );
                        
                        existingHandle.Dispose();
                        _handles.TryRemove(collectionId, out _);
                    }
                    else
                    {
                        _logger.Debug(
                            "Reusing handle for collection {CollectionId}",
                            collectionId
                        );
                        return existingHandle;
                    }
                }
                
                // Create new handle
                _logger.Debug(
                    "Creating new handle for collection {CollectionId} at {FilePath}",
                    collectionId, filePath
                );
                
                var newHandle = new FileStream(
                    filePath,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 8192,      // 8KB buffer
                    useAsync: true         // Enable async I/O
                );
                
                _handles[collectionId] = newHandle;
                return newHandle;
            }
            finally
            {
                collectionLock.Release();
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, 
                "Failed to get handle for collection {CollectionId}",
                collectionId
            );
            throw;
        }
        finally
        {
            _globalLimit.Release();
        }
    }
    
    /// <summary>
    /// Release handle for collection.
    /// </summary>
    public async Task ReleaseHandleAsync(int collectionId)
    {
        ThrowIfDisposed();
        
        if (_handles.TryRemove(collectionId, out var handle))
        {
            try
            {
                await handle.FlushAsync();
                handle.Dispose();
                
                _logger.Debug(
                    "Released handle for collection {CollectionId}",
                    collectionId
                );
            }
            catch (Exception ex)
            {
                _logger.Error(ex,
                    "Error releasing handle for collection {CollectionId}",
                    collectionId
                );
            }
        }
        
        // Remove associated lock
        if (_locks.TryRemove(collectionId, out var semaphore))
        {
            semaphore.Dispose();
        }
    }
    
    /// <summary>
    /// Close handle for collection (alias for ReleaseHandleAsync).
    /// </summary>
    public Task CloseAsync(int collectionId)
    {
        return ReleaseHandleAsync(collectionId);
    }
    
    /// <summary>
    /// Flush all file handles.
    /// </summary>
    public async Task FlushAllAsync()
    {
        ThrowIfDisposed();
        
        var flushTasks = _handles.Values
            .Where(h => h.CanWrite)
            .Select(async handle =>
            {
                try
                {
                    await handle.FlushAsync();
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Error flushing handle");
                }
            });
        
        await Task.WhenAll(flushTasks);
        
        _logger.Debug("Flushed {Count} file handles", _handles.Count);
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        
        // Flush all handles
        try
        {
            FlushAllAsync().Wait(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error flushing handles during disposal");
        }
        
        // Dispose all handles
        foreach (var handle in _handles.Values)
        {
            try
            {
                handle?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error disposing handle");
            }
        }
        
        _handles.Clear();
        
        // Dispose all locks
        foreach (var semaphore in _locks.Values)
        {
            semaphore?.Dispose();
        }
        
        _locks.Clear();
        _globalLimit?.Dispose();
        
        _logger.Information(
            "FileHandlerPool disposed. Released {Count} handles",
            _handles.Count
        );
    }
    
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(FileHandlerPool));
    }
}