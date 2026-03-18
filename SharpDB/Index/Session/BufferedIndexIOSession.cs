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
    private readonly Lock _sync = new();
    private long _nextTempPointer;

    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        lock (_sync)
        {
            if (_cache.TryGetValue(pointer, out var cached))
                return cached;
        }

        var nodeData = await storage.ReadNodeAsync(indexId, pointer);
        var node = nodeFactory.DeserializeNode(nodeData.Bytes);
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

            // If this is a new node (no pointer set), create a unique temporary pointer.
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
        TreeNode<TK>[] dirtyNodes;
        lock (_sync)
        {
            if (_dirtyNodes.Count == 0)
                return;

            dirtyNodes = [.. _dirtyNodes];
            _dirtyNodes.Clear();
        }

        foreach (var node in dirtyNodes)
        {
            var bytes = node.ToBytes();
            var oldPointer = node.Pointer;

            if (node.Pointer.Position < 0)
            {
                var result = await storage.WriteNewNodeAsync(indexId, bytes);
                node.Pointer = result.Pointer;
            }
            else
            {
                await storage.UpdateNodeAsync(indexId, node.Pointer, bytes);
            }

            lock (_sync)
            {
                if (oldPointer != node.Pointer)
                    _cache.Remove(oldPointer);

                _cache[node.Pointer] = node;
            }

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
        }
    }
}
