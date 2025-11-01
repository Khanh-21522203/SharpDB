using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Session;

public class BufferedIndexIOSession<TK>(
    IIndexStorageManager storage,
    INodeFactory<TK, object> nodeFactory,
    int indexId)
    : IIndexIOSession<TK>
    where TK : IComparable<TK>
{
    private readonly Dictionary<Pointer, TreeNode<TK>> _cache = new();
    private readonly HashSet<TreeNode<TK>> _dirtyNodes = new();

    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        if (_cache.TryGetValue(pointer, out var cached))
            return cached;
        
        var nodeData = await storage.ReadNodeAsync(indexId, pointer);
        var node = nodeFactory.DeserializeNode(nodeData.Bytes);
        node.Pointer = pointer;
        
        _cache[pointer] = node;
        return node;
    }
    
    public Task<Pointer> WriteAsync(TreeNode<TK> node)
    {
        _dirtyNodes.Add(node);
        return Task.FromResult(node.Pointer);
    }
    
    public async Task FlushAsync()
    {
        foreach (var node in _dirtyNodes)
        {
            var bytes = node.ToBytes();
            
            if (node.Pointer!.Position == -1)
            {
                var result = await storage.WriteNewNodeAsync(indexId, bytes);
                node.Pointer = result.Pointer;
            }
            else
            {
                await storage.UpdateNodeAsync(indexId, node.Pointer, bytes);
            }
            
            node.ClearModified();
        }
        
        _dirtyNodes.Clear();
    }
    
    public void Dispose()
    {
        FlushAsync().Wait();
        _cache.Clear();
    }
}