using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Page;

public class PageManager : IPageManager
{
    // Inner key is page number; ConcurrentDictionary makes add/lookup thread-safe.
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<int, Page>> _activePages = new();
    private readonly string _basePath;
    private readonly IFileHandlerPool _filePool;
    private readonly ConcurrentDictionary<int, HashSet<long>> _freePages = new();
    private readonly ConcurrentDictionary<int, long> _nextPagePositions = new();
    private readonly int _pageSize;
    private readonly bool _syncOnWrite;
    private readonly LruCache<(int CollectionId, long PagePosition), Page> _pageCache;

    public PageManager(
        string basePath,
        IFileHandlerPool filePool,
        int pageSize = 4096,
        int cacheSize = 1000,
        bool syncOnWrite = true)
    {
        _basePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
        _pageSize = pageSize;
        _syncOnWrite = syncOnWrite;
        _filePool = filePool;
        _pageCache = new LruCache<(int, long), Page>(cacheSize);

        Directory.CreateDirectory(basePath);
    }

    public async Task<Page> AllocatePageAsync(int collectionId)
    {
        // Try reuse free page first
        if (_freePages.TryGetValue(collectionId, out var freeSet) && freeSet.Count > 0)
        {
            var pagePosition = freeSet.First();
            freeSet.Remove(pagePosition);

            var page = await LoadPageAsync(collectionId, pagePosition);
            Array.Clear(page.Data, 0, page.Data.Length);
            page.MarkModified();

            return page;
        }

        // Allocate new page
        var nextPosition = _nextPagePositions.GetOrAdd(collectionId, 0);

        if (nextPosition == 0)
            nextPosition = _pageSize; // Skip page 0 (header)

        var pageNumber = (int)(nextPosition / _pageSize);
        var newPage = new Page(pageNumber, _pageSize, collectionId);

        _activePages.GetOrAdd(collectionId, _ => new ConcurrentDictionary<int, Page>())[pageNumber] = newPage;
        _nextPagePositions[collectionId] = nextPosition + _pageSize;

        return newPage;
    }

    public async Task<Page> LoadPageAsync(int collectionId, long pagePosition)
    {
        // Try to get from cache first
        var cacheKey = (collectionId, pagePosition);
        if (_pageCache.TryGet(cacheKey, out var cachedPage))
        {
            return cachedPage;
        }

        // Check if page is in active pages (in memory but not yet on disk)
        if (_activePages.TryGetValue(collectionId, out var activeSet))
        {
            var targetPageNumber = (int)(pagePosition / _pageSize);
            if (activeSet.TryGetValue(targetPageNumber, out var activePage))
            {
                _pageCache.Put(cacheKey, activePage);
                return activePage;
            }
        }

        // Load from disk
        var filePath = GetFilePath(collectionId);
        var handle = await _filePool.GetHandleAsync(collectionId, filePath);

        // Check if the file is large enough to contain this page
        if (handle.Length <= pagePosition)
        {
            // The page hasn't been written to disk yet - create an empty page
            var emptyPageNumber = (int)(pagePosition / _pageSize);
            var emptyPage = new Page(emptyPageNumber, _pageSize, collectionId);

            _activePages.GetOrAdd(collectionId, _ => new ConcurrentDictionary<int, Page>())[emptyPageNumber] = emptyPage;
            _pageCache.Put(cacheKey, emptyPage);

            return emptyPage;
        }

        var buffer = new byte[_pageSize];
        var bytesRead = await RandomAccess.ReadAsync(handle.SafeFileHandle, buffer, pagePosition);

        if (bytesRead < _pageSize)
            throw new InvalidOperationException($"Incomplete page read. Expected {_pageSize}, got {bytesRead} at position {pagePosition}");

        var pageNumber = (int)(pagePosition / _pageSize);
        var page = new Page(buffer, pageNumber);

        _activePages.GetOrAdd(collectionId, _ => new ConcurrentDictionary<int, Page>())[pageNumber] = page;

        // Add to cache
        _pageCache.Put(cacheKey, page);

        return page;
    }

    public async Task WritePageAsync(int collectionId, Page page)
    {
        var filePath = GetFilePath(collectionId);
        var handle = await _filePool.GetHandleAsync(collectionId, filePath);

        var pagePosition = (long)page.PageNumber * _pageSize;
        handle.Seek(pagePosition, SeekOrigin.Begin);

        await handle.WriteAsync(page.Data, 0, page.Data.Length);
        if (_syncOnWrite)
            await handle.FlushAsync();

        page.ClearModified();

        // Update cache
        _pageCache.Put((collectionId, pagePosition), page);
    }

    public Task FreePageAsync(int collectionId, long pagePosition)
    {
        var freeSet = _freePages.GetOrAdd(collectionId, _ => new HashSet<long>());
        freeSet.Add(pagePosition);

        // Remove from cache
        _pageCache.Remove((collectionId, pagePosition));

        return Task.CompletedTask;
    }

    public async Task<PageStatistics> GetStatisticsAsync(int collectionId)
    {
        var filePath = GetFilePath(collectionId);

        if (!File.Exists(filePath))
            return new PageStatistics
            {
                CollectionId = collectionId,
                TotalPages = 0,
                FreePages = 0,
                UsedPages = 0,
                FileSizeBytes = 0
            };

        var fileInfo = new FileInfo(filePath);
        var totalPages = (int)(fileInfo.Length / _pageSize);
        var freePages = _freePages.TryGetValue(collectionId, out var freeSet)
            ? freeSet.Count
            : 0;

        return new PageStatistics
        {
            CollectionId = collectionId,
            TotalPages = totalPages,
            FreePages = freePages,
            UsedPages = totalPages - freePages,
            FileSizeBytes = fileInfo.Length
        };
    }

    public async Task DisposeCollectionPagesAsync(int collectionId)
    {
        if (_activePages.TryRemove(collectionId, out var pages))
            foreach (var page in pages.Values)
                page.Dispose();

        await _filePool.CloseAsync(collectionId);
    }

    public async Task TruncateCollectionAsync(int collectionId)
    {
        // Clear all in-memory state for this collection
        _activePages.TryRemove(collectionId, out _);
        _freePages.TryRemove(collectionId, out _);
        _nextPagePositions.TryRemove(collectionId, out _);
        // Close file handle then delete the file
        await _filePool.CloseAsync(collectionId);
        var filePath = GetFilePath(collectionId);
        if (File.Exists(filePath))
            File.Delete(filePath);
    }

    private string GetFilePath(int collectionId)
    {
        return Path.Combine(_basePath, $"data_{collectionId}.db");
    }
}