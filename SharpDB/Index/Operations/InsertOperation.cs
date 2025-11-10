using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Operations;

public class InsertOperation<TK, TV>(
    IIndexIOSession<TK> session,
    IIndexStorageManager storage,
    INodeFactory<TK, TV> nodeFactory,
    int indexId)
    where TK : IComparable<TK>
{
    public async Task InsertAsync(TK key, TV value)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);

        if (rootPointer == null)
        {
            var newRoot = nodeFactory.CreateLeafNode();
            newRoot.SetAsRoot();
            newRoot.Insert(key, value);

            var pointer = await session.WriteAsync(newRoot);
            
            // Flush immediately to get the actual pointer
            await session.FlushAsync();
            
            // Update pointer with the actual persisted location
            pointer = newRoot.Pointer;
            
            await storage.SetRootPointerAsync(indexId, pointer);
            return;
        }

        var root = await session.ReadAsync(rootPointer.Value);

        if (root.IsFull())
        {
            // Split root
            var newRoot = nodeFactory.CreateInternalNode();
            newRoot.SetAsRoot();
            root.UnsetAsRoot();
            
            // Set the original root as the first child of the new root
            newRoot.SetChild(0, root.Pointer);

            await SplitChild(newRoot, 0, root);

            var newRootPointer = await session.WriteAsync(newRoot);
            
            // Flush to persist the new root and get the actual pointer
            await session.FlushAsync();
            newRootPointer = newRoot.Pointer;
            
            await storage.SetRootPointerAsync(indexId, newRootPointer);

            await InsertNonFull(newRoot, key, value);
        }
        else
        {
            await InsertNonFull(root, key, value);
        }

        await session.FlushAsync();
    }

    private async Task InsertNonFull(TreeNode<TK> node, TK key, TV value)
    {
        if (node.IsLeaf)
        {
            var leaf = (LeafNode<TK, TV>)node;
            leaf.Insert(key, value);
            await session.WriteAsync(leaf);
        }
        else
        {
            var internalNode = (InternalNode<TK>)node;
            var childPointer = internalNode.FindChild(key);
            var child = await session.ReadAsync(childPointer);

            if (child.IsFull())
            {
                var childIndex = FindChildIndex(internalNode, childPointer);
                await SplitChild(internalNode, childIndex, child);

                // Re-determine which child to insert into
                childPointer = internalNode.FindChild(key);
                child = await session.ReadAsync(childPointer);
            }

            await InsertNonFull(child, key, value);
        }
    }

    private async Task SplitChild(InternalNode<TK> parent, int childIndex, TreeNode<TK> child)
    {
        if (child.IsLeaf)
        {
            var leftLeaf = (LeafNode<TK, TV>)child;
            var rightLeaf = nodeFactory.CreateLeafNode();

            var (rightKeys, rightValues) = leftLeaf.Split();

            for (var i = 0; i < rightKeys.Length; i++) rightLeaf.Insert(rightKeys[i], rightValues[i]);

            rightLeaf.NextLeaf = leftLeaf.NextLeaf;
            var rightPointer = await session.WriteAsync(rightLeaf);
            
            // Flush right leaf to get actual pointer before referencing it
            await session.FlushAsync();
            rightPointer = rightLeaf.Pointer;
            
            leftLeaf.NextLeaf = rightPointer;
            await session.WriteAsync(leftLeaf);

            parent.InsertChild(rightKeys[0], rightPointer);
            await session.WriteAsync(parent);
        }
        else
        {
            var leftInternal = (InternalNode<TK>)child;
            var rightInternal = nodeFactory.CreateInternalNode();

            var rightKeys = leftInternal.SplitAndGetKeys(out var rightChildren);

            // First set the leftmost child
            rightInternal.SetChild(0, rightChildren[0]);
            
            // Then insert keys with their right children
            for (var i = 0; i < rightKeys.Length; i++) rightInternal.InsertChild(rightKeys[i], rightChildren[i + 1]);

            var rightPointer = await session.WriteAsync(rightInternal);
            await session.WriteAsync(leftInternal);
            
            // Flush to get actual pointers
            await session.FlushAsync();
            rightPointer = rightInternal.Pointer;

            var promoteKey = rightKeys.Length > 0 ? rightKeys[0] : default!;
            parent.InsertChild(promoteKey, rightPointer);
            await session.WriteAsync(parent);
        }
    }

    private int FindChildIndex(InternalNode<TK> parent, Pointer childPointer)
    {
        for (var i = 0; i <= parent.KeyCount; i++)
            if (parent.GetChild(i).Equals(childPointer))
                return i;
        return -1;
    }
}