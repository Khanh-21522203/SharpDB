using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Storage;

/// <summary>
///     Optional interface for index storage managers that support session flush registration.
///     Implementations can flush all registered session callbacks when FlushAsync() is called.
/// </summary>
public interface IIndexSessionFlushRegistry
{
    void RegisterSessionFlush(Func<Task> callback);
}

/// <summary>
///     Manages persistent storage for B+ tree index nodes.
/// </summary>
public interface IIndexStorageManager : IDisposable
{
    /// <summary>
    ///     Get root pointer for index.
    /// </summary>
    Task<Pointer?> GetRootPointerAsync(int indexId);

    /// <summary>
    ///     Set root pointer for index.
    /// </summary>
    Task SetRootPointerAsync(int indexId, Pointer pointer);

    /// <summary>
    ///     Read node from storage.
    /// </summary>
    Task<NodeData> ReadNodeAsync(int indexId, Pointer pointer);

    /// <summary>
    ///     Write new node to storage.
    /// </summary>
    Task<NodeData> WriteNewNodeAsync(int indexId, byte[] data);

    /// <summary>
    ///     Update existing node.
    /// </summary>
    Task UpdateNodeAsync(int indexId, Pointer pointer, byte[] data);

    /// <summary>
    ///     Remove node from storage.
    /// </summary>
    Task RemoveNodeAsync(int indexId, Pointer pointer);

    /// <summary>
    ///     Get empty node buffer.
    /// </summary>
    byte[] GetEmptyNode(int keySize, int valueSize, int degree);

    /// <summary>
    ///     Flush deferred writes (e.g. index headers) to durable storage.
    /// </summary>
    Task FlushAsync();

    /// <summary>
    ///     Truncate index: delete metadata, close file handle, delete file.
    /// </summary>
    Task TruncateIndexAsync(int indexId);
}

/// <summary>
///     Encapsulates node data read from storage.
/// </summary>
public record NodeData(Pointer Pointer, byte[] Bytes);