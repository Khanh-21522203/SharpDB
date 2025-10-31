using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Storage.Index.Sessions;

/// <summary>
/// Session that writes changes immediately.
/// No buffering, simple implementation.
/// </summary>
public class ImmediateCommitIndexSession<TK, TV>(
    IIndexStorageManager storage,
    INodeFactory<TK, TV> nodeFactory,
    int indexId) : IIndexIoSession<TK>
    where TK : IComparable<TK>
{
    private readonly IIndexStorageManager _storage = storage;
    private readonly INodeFactory<TK, TV> _nodeFactory = nodeFactory;
    private readonly int _indexId = indexId;
    
    private TreeNode<TK>? _rootCache;

    public async Task<TreeNode<TK>> GetRootAsync()
    {
        if (_rootCache != null)
            return _rootCache;

        var header = await IndexHeaderManager.ReadHeaderAsync(_indexId, _storage);
        if (header.RootPointer == null)
        {
            var emptyNode = _storage.GetEmptyNode(
                header.KeySize,
                header.ValueSize,
                header.Degree);
            var root = _nodeFactory.FromBytes(emptyNode);
            root.SetAsRoot();
            
            // Write immediately
            var pointer = await _storage.WriteAsync(_indexId, root.ToBytes());
            root.Pointer = pointer;
            
            // Update header
            await IndexHeaderManager.UpdateRootPointerAsync(
                _indexId, pointer, _storage
            );
            
            _rootCache = root;
            return root;
        }
        
        // Read existing root
        var nodeData = await _storage.ReadAsync(
            _indexId, 
            header.RootPointer, 
            _storage.GetEmptyNode(header.KeySize, header.ValueSize, header.Degree).Length
        );
        
        var rootNode = _nodeFactory.FromBytes(nodeData);
        rootNode.Pointer = header.RootPointer;
        
        _rootCache = rootNode;
        return rootNode;
    }
    
    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        var header = await IndexHeaderManager.ReadHeaderAsync(_indexId, _storage);
        var nodeSize = _storage.GetEmptyNode(
            header.KeySize, header.ValueSize, header.Degree
        ).Length;
        
        var nodeData = await _storage.ReadAsync(_indexId, pointer, nodeSize);
        var node = _nodeFactory.FromBytes(nodeData);
        node.Pointer = pointer;
        
        return node;
    }
    
    public async Task<Pointer> WriteAsync(TreeNode<TK> node)
    {
        var pointer = await _storage.WriteAsync(_indexId, node.ToBytes());
        node.Pointer = pointer;
        
        // Update root cache if this is new root
        if (node.IsRoot)
        {
            _rootCache = node;
            await IndexHeaderManager.UpdateRootPointerAsync(_indexId, pointer, _storage);
        }
        
        return pointer;
    }
    
    public async Task UpdateAsync(TreeNode<TK> node)
    {
        if (node.Pointer == null)
            throw new InvalidOperationException("Cannot update node without pointer");
        
        if (node.Modified)
        {
            await _storage.UpdateAsync(_indexId, node.Pointer, node.ToBytes());
            node.ClearModified();
        }
        
        // Update root cache
        if (node.IsRoot)
            _rootCache = node;
    }
    
    public async Task RemoveAsync(TreeNode<TK> node)
    {
        if (node.Pointer == null)
            throw new InvalidOperationException("Cannot remove node without pointer");
        
        await _storage.RemoveAsync(_indexId, node.Pointer);
        
        // Clear from cache
        if (node == _rootCache)
            _rootCache = null;
    }
    
    public Task CommitAsync()
    {
        // Everything already written
        return Task.CompletedTask;
    }
    
    public void Dispose()
    {
        // Nothing to dispose
    }
}