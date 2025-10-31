using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Index;

/// <summary>
/// In-memory index storage for testing.
/// </summary>
public class MemoryIndexStorageManager : IIndexStorageManager
{
    private readonly Dictionary<int, byte[]> _headers = new();
    private readonly Dictionary<int, Dictionary<Pointer, byte[]>> _nodes = new();
    private readonly Dictionary<int, long> _nextPositions = new();
    private readonly Lock _lock = new();
    
    public Task<byte[]> ReadHeaderAsync(int indexId)
    {
        lock (_lock)
        {
            if (_headers.TryGetValue(indexId, out var header))
            {
                return Task.FromResult(header);
            }
            
            return Task.FromResult(new byte[IndexHeaderManager.HeaderSize]);
        }
    }
    
    public Task WriteHeaderAsync(int indexId, byte[] header)
    {
        lock (_lock)
        {
            _headers[indexId] = header;
            return Task.CompletedTask;
        }
    }
    
    public Task<int> GetIndexHeaderSizeAsync(int indexId)
    {
        return Task.FromResult(IndexHeaderManager.HeaderSize);
    }
    
    public Task<Pointer> WriteAsync(int indexId, byte[] data)
    {
        lock (_lock)
        {
            if (!_nodes.ContainsKey(indexId))
            {
                _nodes[indexId] = new Dictionary<Pointer, byte[]>();
                _nextPositions[indexId] = 4096; // Skip header page
            }
            
            var position = _nextPositions[indexId];
            _nextPositions[indexId] += 4096; // Increment by page size
            
            var pointer = new Pointer(Pointer.TypeNode, position, 0);
            _nodes[indexId][pointer] = data;
            
            return Task.FromResult(pointer);
        }
    }
    
    public Task<byte[]> ReadAsync(int indexId, Pointer pointer, int length)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(indexId, out var index))
            {
                throw new InvalidOperationException($"Index {indexId} not found");
            }
            
            if (!index.TryGetValue(pointer, out var data))
            {
                throw new InvalidOperationException($"Node at {pointer} not found");
            }
            
            return Task.FromResult(data);
        }
    }
    
    public Task UpdateAsync(int indexId, Pointer pointer, byte[] data)
    {
        lock (_lock)
        {
            if (!_nodes.TryGetValue(indexId, out var index))
            {
                throw new InvalidOperationException($"Index {indexId} not found");
            }
            
            if (!index.ContainsKey(pointer))
            {
                throw new InvalidOperationException($"Node at {pointer} not found");
            }
            
            index[pointer] = data;
            return Task.CompletedTask;
        }
    }
    
    public Task RemoveAsync(int indexId, Pointer pointer)
    {
        lock (_lock)
        {
            if (_nodes.TryGetValue(indexId, out var index))
            {
                index.Remove(pointer);
            }
            
            return Task.CompletedTask;
        }
    }
    
    public byte[] GetEmptyNode(int keySize, int valueSize, int degree)
    {
        int leafSize = 1 + (degree - 1) * (keySize + valueSize) + (2 * Pointer.ByteSize);
        int internalSize = 1 + (degree * Pointer.ByteSize) + ((degree - 1) * keySize);
        
        int nodeSize = Math.Max(leafSize, internalSize);
        return new byte[nodeSize];
    }
    
    public Task FlushAsync()
    {
        // No-op for memory storage
        return Task.CompletedTask;
    }
    
    public void Dispose()
    {
        _headers.Clear();
        _nodes.Clear();
        _nextPositions.Clear();
    }
}