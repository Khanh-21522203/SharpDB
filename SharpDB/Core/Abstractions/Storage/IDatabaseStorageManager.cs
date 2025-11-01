using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Core.Abstractions.Storage;

/// <summary>
/// Manages persistent storage for data records.
/// </summary>
public interface IDatabaseStorageManager : IDisposable
{
    /// <summary>
    /// Store new record, returns pointer to stored data.
    /// </summary>
    Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data);
    
    /// <summary>
    /// Select record by pointer.
    /// </summary>
    Task<DBObject?> SelectAsync(Pointer pointer);
    
    /// <summary>
    /// Update existing record.
    /// </summary>
    Task UpdateAsync(Pointer pointer, byte[] data);
    
    /// <summary>
    /// Delete record (soft delete).
    /// </summary>
    Task DeleteAsync(Pointer pointer);
    
    /// <summary>
    /// Enumerate all records in collection.
    /// </summary>
    IAsyncEnumerable<DBObject> ScanAsync(int collectionId);
    
    /// <summary>
    /// Flush all pending writes.
    /// </summary>
    Task FlushAsync();
}