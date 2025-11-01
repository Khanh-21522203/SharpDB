using System.Collections.Concurrent;
using Serilog;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Storage.Database;

/// <summary>
/// Disk-based storage manager for data records.
/// Uses page-based storage with DBObject wrappers.
/// </summary>
public class DiskPageDatabaseStorageManager(
    IPageManager pageManager,
    ILogger logger,
    IDatabaseHeaderManager headerManager)
    : IDatabaseStorageManager
{
    private readonly IDatabaseHeaderManager _headerManager = headerManager;
    private readonly ConcurrentDictionary<int, Page.Page> _activePages = new();
    private readonly ConcurrentDictionary<int, long> _currentPagePositions = new();
    private bool _disposed;

    public async Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data)
    {
        if (data == null || data.Length == 0)
            throw new ArgumentException("Data cannot be null or empty", nameof(data));
        
        // Get or create current page for collection
        var page = await GetOrCreatePageAsync(collectionId);
        
        // Try allocate in current page
        var dbObject = page.AllocateObject(schemeId, collectionId, version, data);
        
        if (dbObject == null)
        {
            // Current page full, write it and get new page
            await pageManager.WritePageAsync(collectionId, page);
            page = await pageManager.AllocatePageAsync(collectionId);
            _activePages[collectionId] = page;
            
            // Update current page position
            _currentPagePositions[collectionId] = page.PageNumber * 4096; // Assuming 4KB pages
            
            // Retry allocation
            dbObject = page.AllocateObject(schemeId, collectionId, version, data);
            
            if (dbObject == null)
                throw new InvalidOperationException("Failed to allocate object even in new page");
        }
        
        // Create pointer to the object
        var pointer = new Pointer(
            Pointer.TypeData,
            _currentPagePositions[collectionId] + dbObject.Begin,
            0
        );
        
        logger.Debug("Stored {Size} bytes at {Pointer} for collection {CollectionId}",
            data.Length, pointer, collectionId);
        
        return pointer;
    }
    
    public async Task<DBObject?> SelectAsync(Pointer pointer)
    {
        if (pointer.Type != Pointer.TypeData)
            throw new ArgumentException("Pointer must be of TypeData", nameof(pointer));
        
        // Calculate page number and offset
        var pageSize = 4096;
        var pageNumber = (int)(pointer.Position / pageSize);
        var offsetInPage = (int)(pointer.Position % pageSize);
        
        // Load page
        var page = await pageManager.LoadPageAsync(pointer.Chunk, pageNumber * pageSize);
        
        // Get object at offset
        try
        {
            var dbObject = page.GetObjectAt(offsetInPage);
            
            if (!dbObject.IsAlive)
            {
                logger.Verbose("Object at {Pointer} is marked as deleted", pointer);
                return null;
            }
            
            return dbObject;
        }
        catch (ArgumentOutOfRangeException)
        {
            logger.Warning("Invalid offset {Offset} in page {PageNumber}", 
                offsetInPage, pageNumber);
            return null;
        }
    }
    
    public async Task UpdateAsync(Pointer pointer, byte[] data)
    {
        var dbObject = await SelectAsync(pointer);
        
        if (dbObject == null)
            throw new InvalidOperationException($"Object not found at {pointer}");
        
        if (data.Length > dbObject.DataSize)
        {
            throw new InvalidOperationException(
                $"Cannot update with larger data. Current: {dbObject.DataSize}, New: {data.Length}. " +
                "Delete and re-insert instead.");
        }
        
        // Update in-place
        dbObject.ModifyData(data);
        
        // Write modified page
        await pageManager.WritePageAsync(pointer.Chunk, dbObject.Page);
        
        logger.Debug("Updated object at {Pointer}", pointer);
    }
    
    public async Task DeleteAsync(Pointer pointer)
    {
        var dbObject = await SelectAsync(pointer);
        
        if (dbObject == null)
        {
            logger.Warning("Attempted to delete non-existent object at {Pointer}", pointer);
            return;
        }
        
        // Soft delete
        dbObject.MarkDeleted();
        
        // Write modified page
        await pageManager.WritePageAsync(pointer.Chunk, dbObject.Page);
        
        logger.Debug("Deleted object at {Pointer}", pointer);
    }
    
    public async IAsyncEnumerable<DBObject> ScanAsync(int collectionId)
    {
        var statistics = await pageManager.GetStatisticsAsync(collectionId);
        
        // Scan all pages
        for (int pageNum = 0; pageNum < statistics.TotalPages; pageNum++)
        {
            long pagePosition = pageNum * 4096;
            Page.Page page;
            
            try
            {
                page = await pageManager.LoadPageAsync(collectionId, pagePosition);
            }
            catch (Exception ex)
            {
                logger.Warning(ex, "Failed to load page {PageNum} for collection {CollectionId}",
                    pageNum, collectionId);
                continue;
            }
            
            // Enumerate all objects in page
            foreach (var dbObject in page.GetObjects())
            {
                if (dbObject.IsAlive && dbObject.CollectionId == collectionId)
                {
                    yield return dbObject;
                }
            }
        }
    }
    
    public async Task FlushAsync()
    {
        var flushTasks = _activePages.Select(async kvp =>
        {
            if (kvp.Value.Modified)
            {
                await pageManager.WritePageAsync(kvp.Key, kvp.Value);
            }
        });
        
        await Task.WhenAll(flushTasks);
        
        logger.Information("Flushed {Count} active pages", _activePages.Count);
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        
        // Flush all pages
        FlushAsync().Wait();
        
        // Clear active pages
        foreach (var page in _activePages.Values)
        {
            page.Dispose();
        }
        
        _activePages.Clear();
        
        logger.Information("DiskPageDatabaseStorageManager disposed");
    }
    
    private async Task<Page.Page> GetOrCreatePageAsync(int collectionId)
    {
        if (_activePages.TryGetValue(collectionId, out var page))
            return page;
        
        // Allocate new page
        page = await pageManager.AllocatePageAsync(collectionId);
        _activePages[collectionId] = page;
        _currentPagePositions[collectionId] = page.PageNumber * 4096;
        
        return page;
    }
}