using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;

namespace SharpDB.Storage.Page;

public class PageManager : IPageManager
{
    private readonly ConcurrentDictionary<int, HashSet<Page>> _activePages = new();
    private readonly string _basePath;
    private readonly IFileHandlerPool _filePool;
    private readonly ConcurrentDictionary<int, HashSet<long>> _freePages = new();
    private readonly ConcurrentDictionary<int, long> _nextPagePositions = new();
    private readonly int _pageSize;

    public PageManager(
        string basePath,
        IFileHandlerPool filePool,
        int pageSize = 4096)
    {
        _basePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
        _pageSize = pageSize;
        _filePool = filePool;

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
        var newPage = new Page(pageNumber, _pageSize);

        _activePages.GetOrAdd(collectionId, _ => new HashSet<Page>()).Add(newPage);
        _nextPagePositions[collectionId] = nextPosition + _pageSize;

        return newPage;
    }

    public async Task<Page> LoadPageAsync(int collectionId, long pagePosition)
    {
        var filePath = GetFilePath(collectionId);
        var handle = await _filePool.GetHandleAsync(collectionId, filePath);

        handle.Seek(pagePosition, SeekOrigin.Begin);

        var buffer = new byte[_pageSize];
        var bytesRead = await handle.ReadAsync(buffer, 0, _pageSize);

        if (bytesRead < _pageSize)
            throw new InvalidOperationException($"Incomplete page read. Expected {_pageSize}, got {bytesRead}");

        var pageNumber = (int)(pagePosition / _pageSize);
        var page = new Page(buffer, pageNumber);

        _activePages.GetOrAdd(collectionId, _ => new HashSet<Page>()).Add(page);

        return page;
    }

    public async Task WritePageAsync(int collectionId, Page page)
    {
        var filePath = GetFilePath(collectionId);
        var handle = await _filePool.GetHandleAsync(collectionId, filePath);

        var pagePosition = page.PageNumber * _pageSize;
        handle.Seek(pagePosition, SeekOrigin.Begin);

        await handle.WriteAsync(page.Data, 0, page.Data.Length);
        await handle.FlushAsync();

        page.ClearModified();
    }

    public Task FreePageAsync(int collectionId, long pagePosition)
    {
        var freeSet = _freePages.GetOrAdd(collectionId, _ => new HashSet<long>());
        freeSet.Add(pagePosition);

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
            foreach (var page in pages)
                page.Dispose();

        await _filePool.CloseAsync(collectionId);
    }

    private string GetFilePath(int collectionId)
    {
        return Path.Combine(_basePath, $"data_{collectionId}.db");
    }
}