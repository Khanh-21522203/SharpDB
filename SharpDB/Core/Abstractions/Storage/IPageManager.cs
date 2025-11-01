using SharpDB.Storage.Page;

namespace SharpDB.Core.Abstractions.Storage;

/// <summary>
///     Manages page allocation and lifecycle.
/// </summary>
public interface IPageManager
{
    /// <summary>
    ///     Allocate new page.
    /// </summary>
    Task<Page> AllocatePageAsync(int collectionId);

    /// <summary>
    ///     Load existing page.
    /// </summary>
    Task<Page> LoadPageAsync(int collectionId, long pagePosition);

    /// <summary>
    ///     Write page to storage.
    /// </summary>
    Task WritePageAsync(int collectionId, Page page);

    /// <summary>
    ///     Free page (mark as available).
    /// </summary>
    Task FreePageAsync(int collectionId, long pagePosition);

    /// <summary>
    ///     Get page statistics.
    /// </summary>
    Task<PageStatistics> GetStatisticsAsync(int collectionId);

    /// <summary>
    ///     Dispose all pages for collection.
    ///     Breaks circular references and releases resources.
    /// </summary>
    Task DisposeCollectionPagesAsync(int collectionId);
}