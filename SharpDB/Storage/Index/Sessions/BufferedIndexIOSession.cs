using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Storage.Index.Sessions;

public class BufferedIndexIoSession<TK, TV>(IIndexStorageManager storage, INodeFactory<TK, TV> nodeFactory, int indexId)
    : IIndexIoSession<TK>
    where TK : IComparable<TK>
{
    private readonly IIndexStorageManager _storage = storage;
    private readonly INodeFactory<TK, TV> _nodeFactory = nodeFactory;
    private readonly int _indexId = indexId;
    
    // Caches
    private TreeNode<TK>? _rootCache;
    private readonly Dictionary<Pointer, TreeNode<TK>> _nodeCache = new();
    // Pending operations
    private readonly List<TreeNode<TK>> _nodesToWrite = new();
    private readonly List<TreeNode<TK>> _nodesToUpdate = new();
    private readonly List<Pointer> _nodesToRemove = new();
    
    public async Task<TreeNode<TK>> GetRootAsync()
    {
        if (_rootCache != null)
            return _rootCache;

        var header = await IndexHeaderManager.ReadHeaderAsync(_indexId, _storage);
        if (header.RootPointer == null)
        {
            var emptyNode = _storage.GetEmptyNode(header.KeySize, header.ValueSize, header.Degree);
            var root = _nodeFactory.FromBytes(emptyNode);
            root.SetAsRoot();
            
            // Cache new root
            _rootCache = root;
            return root;
        }
        
        // Check cache
        if (_nodeCache.TryGetValue(header.RootPointer, out var cachedRoot))
        {
            _rootCache = cachedRoot;
            return cachedRoot;
        }
        
        // Read from storage
        var nodeSize = _storage.GetEmptyNode(header.KeySize, header.ValueSize, header.Degree).Length;
        
        var nodeData = await _storage.ReadAsync(_indexId, header.RootPointer, nodeSize);
        
        var rootNode = _nodeFactory.FromBytes(nodeData);
        rootNode.Pointer = header.RootPointer;
        
        _rootCache = rootNode;
        _nodeCache[header.RootPointer] = rootNode;
        
        return rootNode;
    }
    
    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        // Check cache first
        if (_nodeCache.TryGetValue(pointer, out var cachedNode))
            return cachedNode;
        
        // Read from storage
        var header = await IndexHeaderManager.ReadHeaderAsync(_indexId, _storage);
        var nodeSize = _storage.GetEmptyNode(header.KeySize, header.ValueSize, header.Degree).Length;
        
        var nodeData = await _storage.ReadAsync(_indexId, pointer, nodeSize);
        var node = _nodeFactory.FromBytes(nodeData);
        node.Pointer = pointer;
        
        // Cache it
        _nodeCache[pointer] = node;
        
        return node;
    }
    
    public Task<Pointer> WriteAsync(TreeNode<TK> node)
    {
        // Assign temporary pointer (will be replaced at commit)
        var tempPointer = new Pointer(
            Pointer.TypeNode,
            -(_nodesToWrite.Count + 1), // Negative position for temp
            0
        );
        
        node.Pointer = tempPointer;
        _nodesToWrite.Add(node);
        _nodeCache[tempPointer] = node;
        
        if (node.IsRoot)
        {
            _rootCache = node;
        }
        
        return Task.FromResult(tempPointer);
    }
    
    public Task UpdateAsync(TreeNode<TK> node)
    {
        if (node.Pointer == null)
            throw new InvalidOperationException("Cannot update node without pointer");
        
        if (node.Modified && !_nodesToWrite.Contains(node))
        {
            _nodesToUpdate.Add(node);
        }
        
        if (node.IsRoot)
        {
            _rootCache = node;
        }
        
        return Task.CompletedTask;
    }
    
    public Task RemoveAsync(TreeNode<TK> node)
    {
        if (node.Pointer == null)
            throw new InvalidOperationException("Cannot remove node without pointer");
        
        _nodesToRemove.Add(node.Pointer);
        _nodeCache.Remove(node.Pointer);
        
        if (node == _rootCache)
        {
            _rootCache = null;
        }
        
        return Task.CompletedTask;
    }
    
    /// <summary>
    /// Commit all buffered changes atomically.
    /// </summary>
    public async Task CommitAsync()
    {
        // Map temp pointers to real pointers
        // This is needed because parent nodes may reference temp pointers
        // that need to be updated to real pointers after write
        var pointerMap = new Dictionary<Pointer, Pointer>();
        
        // Write new nodes
        foreach (var node in _nodesToWrite)
        {
            var tempPointer = node.Pointer!;
            var realPointer = await _storage.WriteAsync(_indexId, node.ToBytes());
            
            pointerMap[tempPointer] = realPointer;
            node.Pointer = realPointer;
            
            // Update cache with real pointer
            _nodeCache.Remove(tempPointer);
            _nodeCache[realPointer] = node;
        }
        
        // Update child pointers in all nodes that reference new nodes
        foreach (var node in _nodeCache.Values)
        {
            if (node is not InternalNode<TK> internalNode) continue;
            
            // Update child pointers if they were temporary
            var childCount = internalNode.GetKeys().Count + 1;
            for (var i = 0; i < childCount; i++)
            {
                var childPtr = internalNode.GetChildAt(i);
                    
                // Check if this pointer was a temp pointer
                if (pointerMap.TryGetValue(childPtr, out var realPtr))
                {
                    internalNode.SetChildAt(i, realPtr);
                }
            }
        }
        
        // Update existing nodes
        foreach (var node in _nodesToUpdate)
        {
            await _storage.UpdateAsync(_indexId, node.Pointer!, node.ToBytes());
            node.ClearModified();
        }
        
        // Remove nodes
        foreach (var pointer in _nodesToRemove)
        {
            await _storage.RemoveAsync(_indexId, pointer);
        }
        
        // Update root pointer if changed
        if (_rootCache?.Pointer != null && _rootCache.IsRoot)
        {
            await IndexHeaderManager.UpdateRootPointerAsync(
                _indexId, _rootCache.Pointer, _storage
            );
        }
        
        // Clear buffers
        _nodesToWrite.Clear();
        _nodesToUpdate.Clear();
        _nodesToRemove.Clear();
    }
    
    public void Dispose()
    {
        _nodeCache.Clear();
        _nodesToWrite.Clear();
        _nodesToUpdate.Clear();
        _nodesToRemove.Clear();
    }
}