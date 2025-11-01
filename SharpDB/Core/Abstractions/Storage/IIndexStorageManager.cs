using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Storage;

/// <summary>
/// Manages persistent storage for B+ tree index nodes.
/// </summary>
public interface IIndexStorageManager : IDisposable
{
    /// <summary>
    /// Get root pointer for index.
    /// </summary>
    Task<Pointer?> GetRootPointerAsync(int indexId);
    
    /// <summary>
    /// Set root pointer for index.
    /// </summary>
    Task SetRootPointerAsync(int indexId, Pointer pointer);
    
    /// <summary>
    /// Read node from storage.
    /// </summary>
    Task<NodeData> ReadNodeAsync(int indexId, Pointer pointer);
    
    /// <summary>
    /// Write new node to storage.
    /// </summary>
    Task<NodeData> WriteNewNodeAsync(int indexId, byte[] data);
    
    /// <summary>
    /// Update existing node.
    /// </summary>
    Task UpdateNodeAsync(int indexId, Pointer pointer, byte[] data);
    
    /// <summary>
    /// Remove node from storage.
    /// </summary>
    Task RemoveNodeAsync(int indexId, Pointer pointer);
    
    /// <summary>
    /// Get empty node buffer.
    /// </summary>
    byte[] GetEmptyNode(int keySize, int valueSize, int degree);
}

/// <summary>
/// Encapsulates node data read from storage.
/// </summary>
public record NodeData(Pointer Pointer, byte[] Bytes);