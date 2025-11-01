namespace SharpDB.Core.Abstractions.Storage;

/// <summary>
/// Pool of file handles for efficient I/O.
/// </summary>
public interface IFileHandlerPool : IDisposable
{
    /// <summary>
    /// Get or create file handle.
    /// </summary>
    Task<FileStream> GetHandleAsync(int collectionId, string filePath);
    
    /// <summary>
    /// Release handle for collection.
    /// </summary>
    Task ReleaseHandleAsync(int collectionId);
    
    /// <summary>
    /// Flush all handles.
    /// </summary>
    Task FlushAllAsync();
}