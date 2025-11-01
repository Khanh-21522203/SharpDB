using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Session;

public class ImmediateCommitIndexSession<TK>(
    IIndexStorageManager storage,
    INodeFactory<TK, object> nodeFactory,
    int indexId)
    : IIndexIOSession<TK>
    where TK : IComparable<TK>
{
    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        var nodeData = await storage.ReadNodeAsync(indexId, pointer);
        var node = nodeFactory.DeserializeNode(nodeData.Bytes);
        node.Pointer = pointer;
        return node;
    }

    public async Task<Pointer> WriteAsync(TreeNode<TK> node)
    {
        var bytes = node.ToBytes();

        await storage.UpdateNodeAsync(indexId, node.Pointer, bytes);
        return node.Pointer;
    }

    public Task FlushAsync()
    {
        return Task.CompletedTask;
        // Already flushed
    }

    public void Dispose()
    {
    }
}