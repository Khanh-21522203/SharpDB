using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Index;

/// <summary>
/// In-memory index storage for testing and development.
/// No persistence - data lost on restart.
/// </summary>
public class MemoryIndexStorageManager : IIndexStorageManager
{
    private readonly ConcurrentDictionary<int, Pointer?> _rootPointers = new();
    private readonly ConcurrentDictionary<string, byte[]> _nodes = new();
    private long _nextPosition = 0;
    
    public Task<Pointer?> GetRootPointerAsync(int indexId)
    {
        _rootPointers.TryGetValue(indexId, out var pointer);
        return Task.FromResult(pointer);
    }
    
    public Task SetRootPointerAsync(int indexId, Pointer pointer)
    {
        _rootPointers[indexId] = pointer;
        return Task.CompletedTask;
    }
    
    public Task<NodeData> ReadNodeAsync(int indexId, Pointer pointer)
    {
        var key = GetNodeKey(indexId, pointer);
        
        if (!_nodes.TryGetValue(key, out var data))
            throw new InvalidOperationException($"Node not found: {key}");
        
        return Task.FromResult(new NodeData(pointer, data));
    }
    
    public Task<NodeData> WriteNewNodeAsync(int indexId, byte[] data)
    {
        var position = Interlocked.Increment(ref _nextPosition);
        var pointer = new Pointer(Pointer.TypeNode, position, 0);
        var key = GetNodeKey(indexId, pointer);
        
        _nodes[key] = data;
        
        return Task.FromResult(new NodeData(pointer, data));
    }
    
    public Task UpdateNodeAsync(int indexId, Pointer pointer, byte[] data)
    {
        var key = GetNodeKey(indexId, pointer);
        _nodes[key] = data;
        
        return Task.CompletedTask;
    }
    
    public Task RemoveNodeAsync(int indexId, Pointer pointer)
    {
        var key = GetNodeKey(indexId, pointer);
        _nodes.TryRemove(key, out _);
        
        return Task.CompletedTask;
    }
    
    public byte[] GetEmptyNode(int keySize, int valueSize, int degree)
    {
        var headerSize = 6;
        var keysSize = degree * keySize;
        var valuesSize = (degree + 1) * Math.Max(valueSize, Pointer.ByteSize);
        var totalSize = headerSize + keysSize + valuesSize;
        
        return new byte[totalSize + 4];
    }
    
    public void Dispose()
    {
        _nodes.Clear();
        _rootPointers.Clear();
    }
    
    private string GetNodeKey(int indexId, Pointer pointer)
    {
        return $"{indexId}:{pointer.Position}";
    }
}