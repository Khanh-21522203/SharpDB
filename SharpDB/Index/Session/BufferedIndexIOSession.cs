using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Session;

public class BufferedIndexIOSession<TK> : IIndexIOSession<TK>
    where TK : IComparable<TK>
{
    private readonly IIndexStorageManager _storage;
    private readonly INodeFactory<TK, object> _nodeFactory;
    private readonly int _indexId;
    private readonly Dictionary<Pointer, TreeNode<TK>> _cache = new();
    private readonly HashSet<TreeNode<TK>> _dirtyNodes = new();
    private readonly Lock _sync = new();
    // Reusable lists for FlushAsync — avoids LINQ ToList() allocations per flush.
    // Protected by _flushLock (serialized flush) so concurrent FlushAsync calls don't corrupt them.
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private readonly List<TreeNode<TK>> _flushBuffer = new();
    private readonly List<TreeNode<TK>> _newNodesBuffer = new();
    private readonly List<TreeNode<TK>> _updatedNodesBuffer = new();
    private long _nextTempPointer;

    public BufferedIndexIOSession(
        IIndexStorageManager storage,
        INodeFactory<TK, object> nodeFactory,
        int indexId)
    {
        _storage = storage;
        _nodeFactory = nodeFactory;
        _indexId = indexId;

        // Auto-register with the storage manager so transaction-level flush triggers this session.
        if (storage is IIndexSessionFlushRegistry registry)
            registry.RegisterSessionFlush(() => FlushAsync());
    }

    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        lock (_sync)
        {
            if (_cache.TryGetValue(pointer, out var cached))
                return cached;
        }

        var nodeData = await _storage.ReadNodeAsync(_indexId, pointer);
        var node = _nodeFactory.DeserializeNode(nodeData.Bytes);
        node.Pointer = pointer;

        lock (_sync)
        {
            if (_cache.TryGetValue(pointer, out var existing))
                return existing;

            _cache[pointer] = node;
        }

        return node;
    }

    public Task<Pointer> WriteAsync(TreeNode<TK> node)
    {
        lock (_sync)
        {
            _dirtyNodes.Add(node);

            // If this is a new node (no pointer set), assign a unique temporary pointer.
            if (node.Pointer.Position == 0 && node.Pointer.Type == 0)
            {
                var tempPointer = Interlocked.Decrement(ref _nextTempPointer);
                node.Pointer = new Pointer(Pointer.TypeNode, tempPointer, 0);
            }

            _cache[node.Pointer] = node;
            return Task.FromResult(node.Pointer);
        }
    }

    public async Task FlushAsync()
    {
        await _flushLock.WaitAsync();
        try
        {
            await FlushLockedAsync();
        }
        finally
        {
            _flushLock.Release();
        }
    }

    private async Task FlushLockedAsync()
    {
        lock (_sync)
        {
            if (_dirtyNodes.Count == 0)
                return;

            _flushBuffer.Clear();
            _flushBuffer.AddRange(_dirtyNodes);
            _dirtyNodes.Clear();
        }

        // Partition into new (temp, Position < 0) and updated (real, Position >= 0) nodes.
        _newNodesBuffer.Clear();
        _updatedNodesBuffer.Clear();
        foreach (var n in _flushBuffer)
        {
            if (n.Pointer.Position < 0)
                _newNodesBuffer.Add(n);
            else
                _updatedNodesBuffer.Add(n);
        }

        // Phase 1: write new nodes in child-first order.
        // Temp pointers are assigned via Interlocked.Decrement from 0, so children (created
        // first during splits) have higher (less-negative) values than their parents.
        // Sorting descending puts less-negative (children) before more-negative (parents).
        _newNodesBuffer.Sort(static (a, b) => b.Pointer.Position.CompareTo(a.Pointer.Position));

        foreach (var node in _newNodesBuffer)
        {
            var bytes = node.ToBytes();
            var oldPointer = node.Pointer;

            var result = await _storage.WriteNewNodeAsync(_indexId, bytes);
            var realPointer = result.Pointer;
            node.Pointer = realPointer;

            // Patch all dirty nodes that hold the old temp pointer as a child/NextLeaf reference.
            foreach (var other in _flushBuffer)
                other.PatchPointer(oldPointer, realPointer);

            lock (_sync)
            {
                _cache.Remove(oldPointer);
                _cache[realPointer] = node;
            }

            node.ClearModified();
        }

        // Phase 2: write updated nodes (real pointers; may have been patched in phase 1).
        foreach (var node in _updatedNodesBuffer)
        {
            var bytes = node.ToBytes();
            await _storage.UpdateNodeAsync(_indexId, node.Pointer, bytes);

            lock (_sync)
                _cache[node.Pointer] = node;

            node.ClearModified();
        }
    }

    public void Dispose()
    {
        FlushAsync().Wait();
        lock (_sync)
        {
            _cache.Clear();
            _dirtyNodes.Clear();
            _flushBuffer.Clear();
            _newNodesBuffer.Clear();
            _updatedNodesBuffer.Clear();
        }
    }
}
