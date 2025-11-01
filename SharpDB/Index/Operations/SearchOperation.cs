using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Operations;

public class SearchOperation<TK, TV>(
    IIndexIOSession<TK> session,
    IIndexStorageManager storage,
    int indexId)
    where TK : IComparable<TK>
{
    public async Task<TV?> SearchAsync(TK key)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);
        if (rootPointer == null)
            return default;

        var leaf = await FindLeafAsync(key, rootPointer.Value);

        if (leaf.TryGetValue(key, out var value))
            return value;

        return default;
    }

    public async IAsyncEnumerable<KeyValue<TK, TV>> RangeSearchAsync(TK minKey, TK maxKey)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);
        if (rootPointer == null)
            yield break;

        var leaf = await FindLeafAsync(minKey, rootPointer.Value);

        while (leaf != null)
        {
            for (var i = 0; i < leaf.KeyCount; i++)
            {
                var key = leaf.GetKeyAt(i);

                if (key.CompareTo(minKey) < 0)
                    continue;

                if (key.CompareTo(maxKey) > 0)
                    yield break;

                if (leaf.TryGetValue(key, out var value))
                    yield return new KeyValue<TK, TV>(key, value!);
            }

            if (leaf.NextLeaf == null)
                break;

            var nextNode = await session.ReadAsync(leaf.NextLeaf.Value);
            leaf = nextNode as LeafNode<TK, TV>;
        }
    }

    private async Task<LeafNode<TK, TV>> FindLeafAsync(TK key, Pointer startPointer)
    {
        var current = await session.ReadAsync(startPointer);

        while (!current.IsLeaf)
        {
            var internalNode = (InternalNode<TK>)current;
            var childPointer = internalNode.FindChild(key);
            current = await session.ReadAsync(childPointer);
        }

        return (LeafNode<TK, TV>)current;
    }
}