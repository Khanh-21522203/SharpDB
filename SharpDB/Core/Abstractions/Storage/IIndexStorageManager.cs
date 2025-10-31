using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Storage;

/// <summary>
/// Manages persistent storage of B+ Tree nodes.
/// </summary>
public interface IIndexStorageManager : IDisposable
{
    // Header operations
    Task<byte[]> ReadHeaderAsync(int indexId);
    Task WriteHeaderAsync(int indexId, byte[] header);
    Task<int> GetIndexHeaderSizeAsync(int indexId);
    
    // Node operations
    Task<Pointer> WriteAsync(int indexId, byte[] data);
    Task<byte[]> ReadAsync(int indexId, Pointer pointer, int length);
    Task UpdateAsync(int indexId, Pointer pointer, byte[] data);
    Task RemoveAsync(int indexId, Pointer pointer);
    
    // Utility
    byte[] GetEmptyNode(int keySize, int valueSize, int degree);
    Task FlushAsync();
}