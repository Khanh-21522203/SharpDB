using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Manager;

public interface IUniqueQueryableIndex<TK, TV> : IUniqueTreeIndexManager<TK, TV>, IQueryable<TK, TV>
    where TK : IComparable<TK>
{
}

public class UniqueQueryableIndexDecorator<TK, TV>(
    IUniqueTreeIndexManager<TK, TV> inner,
    IIndexIOSession<TK> session,
    IIndexStorageManager storage,
    int indexId)
    : IUniqueQueryableIndex<TK, TV>
    where TK : IComparable<TK>
{
    public async IAsyncEnumerable<KeyValue<TK, TV>> GreaterThanAsync(TK key)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);
        if (rootPointer == null)
            yield break;

        // Find leaf containing key
        var leaf = await FindLeafAsync(key, rootPointer.Value);

        // Scan from key to end
        while (leaf != null)
        {
            for (var i = 0; i < leaf.KeyCount; i++)
            {
                var currentKey = leaf.GetKeyAt(i);

                // Skip keys <= key (we want strictly greater)
                if (currentKey.CompareTo(key) <= 0)
                    continue;

                if (leaf.TryGetValue(currentKey, out var value))
                    yield return new KeyValue<TK, TV>(currentKey, value!);
            }

            // Move to next leaf
            if (leaf.NextLeaf == null)
                break;

            var nextNode = await session.ReadAsync(leaf.NextLeaf.Value);
            leaf = nextNode as LeafNode<TK, TV>;
        }
    }

    public async IAsyncEnumerable<KeyValue<TK, TV>> LessThanAsync(TK key)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);
        if (rootPointer == null)
            yield break;

        // Find leftmost leaf (start of tree)
        var leaf = await FindLeftmostLeafAsync(rootPointer.Value);

        // Scan from start to key
        while (leaf != null)
        {
            for (var i = 0; i < leaf.KeyCount; i++)
            {
                var currentKey = leaf.GetKeyAt(i);

                // Stop when we reach keys >= key (we want strictly less)
                if (currentKey.CompareTo(key) >= 0)
                    yield break;

                if (leaf.TryGetValue(currentKey, out var value))
                    yield return new KeyValue<TK, TV>(currentKey, value!);
            }

            // Move to next leaf
            if (leaf.NextLeaf == null)
                break;

            var nextNode = await session.ReadAsync(leaf.NextLeaf.Value);
            leaf = nextNode as LeafNode<TK, TV>;
        }
    }

    public async IAsyncEnumerable<KeyValue<TK, TV>> RangeAsync(TK minKey, TK maxKey)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);
        if (rootPointer == null)
            yield break;

        // Find leaf containing minKey
        var leaf = await FindLeafAsync(minKey, rootPointer.Value);

        // Scan from minKey to maxKey
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

    // Delegate to inner manager
    public Task<TV?> GetAsync(TK key)
    {
        return inner.GetAsync(key);
    }

    public Task PutAsync(TK key, TV value)
    {
        return inner.PutAsync(key, value);
    }

    public Task<bool> RemoveAsync(TK key)
    {
        return inner.RemoveAsync(key);
    }

    public Task<bool> ContainsKeyAsync(TK key)
    {
        return inner.ContainsKeyAsync(key);
    }

    public Task<int> CountAsync()
    {
        return inner.CountAsync();
    }

    public Task FlushAsync()
    {
        return inner.FlushAsync();
    }

    public void Dispose()
    {
        inner.Dispose();
    }

    private async Task<LeafNode<TK, TV>> FindLeafAsync(TK key, Pointer rootPointer)
    {
        var node = await session.ReadAsync(rootPointer);

        while (!node.IsLeaf)
        {
            var internalNode = (InternalNode<TK>)node;
            var childPointer = internalNode.FindChild(key);
            node = await session.ReadAsync(childPointer);
        }

        return (LeafNode<TK, TV>)node;
    }

    private async Task<LeafNode<TK, TV>> FindLeftmostLeafAsync(Pointer rootPointer)
    {
        var node = await session.ReadAsync(rootPointer);

        while (!node.IsLeaf)
        {
            var internalNode = (InternalNode<TK>)node;
            var childPointer = internalNode.GetChild(0); // Leftmost child
            node = await session.ReadAsync(childPointer);
        }

        return (LeafNode<TK, TV>)node;
    }
}