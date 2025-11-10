using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Index.Operations;

public class DeleteOperation<TK, TV>(
    IIndexIOSession<TK> session,
    IIndexStorageManager storage,
    int indexId)
    where TK : IComparable<TK>
{
    public async Task<bool> DeleteAsync(TK key)
    {
        var rootPointer = await storage.GetRootPointerAsync(indexId);
        if (rootPointer == null)
            return false;

        var root = await session.ReadAsync(rootPointer.Value);
        var deleted = await DeleteFromNode(root, key);

        // If root is now empty and has children, promote child
        if (root is { KeyCount: 0, IsLeaf: false })
        {
            var internalRoot = (InternalNode<TK>)root;
            var newRootPointer = internalRoot.GetChild(0);
            var newRoot = await session.ReadAsync(newRootPointer);
            newRoot.SetAsRoot();

            await session.WriteAsync(newRoot);
            await storage.SetRootPointerAsync(indexId, newRootPointer);
        }

        await session.FlushAsync();
        return deleted;
    }

    private async Task<bool> DeleteFromNode(TreeNode<TK> node, TK key)
    {
        if (node.IsLeaf)
        {
            var leaf = (LeafNode<TK, TV>)node;
            var removed = leaf.Remove(key);

            if (removed)
                await session.WriteAsync(leaf);

            return removed;
        }

        var internalNode = (InternalNode<TK>)node;
        var childPointer = internalNode.FindChild(key);
        var child = await session.ReadAsync(childPointer);

        var deleted = await DeleteFromNode(child, key);

        if (child.IsMinimum() && !child.IsRoot)
        {
            await RebalanceChild(internalNode, child, childPointer);
            
            // After rebalance, parent might need updating
            await session.WriteAsync(internalNode);
        }

        return deleted;
    }

    private async Task RebalanceChild(InternalNode<TK> parent, TreeNode<TK> child, Pointer childPointer)
    {
        // Don't rebalance if parent is root with only one child (will be handled by caller)
        if (parent.IsRoot && parent.KeyCount == 0)
            return;
        
        // If parent has no keys, it should have exactly one child - nothing to rebalance
        if (parent.KeyCount == 0)
            return;
            
        var childIndex = FindChildIndex(parent, childPointer);

        // Try borrow from left sibling
        if (childIndex > 0)
        {
            var leftSiblingPointer = parent.GetChild(childIndex - 1);
            var leftSibling = await session.ReadAsync(leftSiblingPointer);

            if (!leftSibling.IsMinimum())
            {
                await BorrowFromLeft(parent, child, leftSibling, childIndex);
                return;
            }
        }

        // Try borrow from right sibling
        if (childIndex < parent.KeyCount)
        {
            var rightSiblingPointer = parent.GetChild(childIndex + 1);
            var rightSibling = await session.ReadAsync(rightSiblingPointer);

            if (!rightSibling.IsMinimum())
            {
                await BorrowFromRight(parent, child, rightSibling, childIndex);
                return;
            }
        }

        // Merge with sibling
        if (childIndex > 0)
        {
            var leftSiblingPointer = parent.GetChild(childIndex - 1);
            var leftSibling = await session.ReadAsync(leftSiblingPointer);
            await MergeWithLeft(parent, child, leftSibling, childIndex);
        }
        else if (childIndex < parent.KeyCount)
        {
            var rightSiblingPointer = parent.GetChild(childIndex + 1);
            var rightSibling = await session.ReadAsync(rightSiblingPointer);
            await MergeWithRight(parent, child, rightSibling, childIndex);
        }
    }

    private async Task BorrowFromLeft(InternalNode<TK> parent, TreeNode<TK> child, TreeNode<TK> leftSibling,
        int childIndex)
    {
        if (child.IsLeaf)
        {
            var childLeaf = (LeafNode<TK, TV>)child;
            var leftLeaf = (LeafNode<TK, TV>)leftSibling;

            // Move last item from left to first of child
            var lastKey = leftLeaf.GetKeyAt(leftLeaf.KeyCount - 1);
            leftLeaf.TryGetValue(lastKey, out var lastValue);
            leftLeaf.Remove(lastKey);

            childLeaf.Insert(lastKey, lastValue!);

            // Update parent separator key
            parent.SetKeyAt(childIndex - 1, childLeaf.GetKeyAt(0));

            await session.WriteAsync(leftLeaf);
            await session.WriteAsync(childLeaf);
            await session.WriteAsync(parent);
        }
    }

    private async Task BorrowFromRight(InternalNode<TK> parent, TreeNode<TK> child, TreeNode<TK> rightSibling,
        int childIndex)
    {
        if (child.IsLeaf)
        {
            var childLeaf = (LeafNode<TK, TV>)child;
            var rightLeaf = (LeafNode<TK, TV>)rightSibling;

            // Move first item from right to last of child
            var firstKey = rightLeaf.GetKeyAt(0);
            rightLeaf.TryGetValue(firstKey, out var firstValue);
            rightLeaf.Remove(firstKey);

            childLeaf.Insert(firstKey, firstValue!);

            // Update parent separator key
            parent.SetKeyAt(childIndex, rightLeaf.GetKeyAt(0));

            await session.WriteAsync(childLeaf);
            await session.WriteAsync(rightLeaf);
            await session.WriteAsync(parent);
        }
    }

    private async Task MergeWithLeft(InternalNode<TK> parent, TreeNode<TK> child, TreeNode<TK> leftSibling,
        int childIndex)
    {
        if (child.IsLeaf)
        {
            var childLeaf = (LeafNode<TK, TV>)child;
            var leftLeaf = (LeafNode<TK, TV>)leftSibling;

            // Check if merge is possible (total keys must fit in one node)
            var totalKeys = leftLeaf.KeyCount + childLeaf.KeyCount;
            if (totalKeys > leftLeaf.Degree)
            {
                // Cannot merge - would exceed capacity
                return;
            }

            // Merge child into left sibling using direct copy (no Insert checks)
            leftLeaf.MergeFrom(childLeaf);

            leftLeaf.NextLeaf = childLeaf.NextLeaf;

            // Remove separator from parent
            parent.RemoveKey(childIndex - 1);

            await session.WriteAsync(leftLeaf);
            await session.WriteAsync(parent);
        }
        else
        {
            var childInternal = (InternalNode<TK>)child;
            var leftInternal = (InternalNode<TK>)leftSibling;

            // Check capacity: left keys + separator + child keys must fit
            var totalKeys = leftInternal.KeyCount + 1 + childInternal.KeyCount; // +1 for separator
            if (totalKeys > leftInternal.Degree)
            {
                // Cannot merge - would exceed capacity
                return;
            }

            // Get separator key from parent
            var separatorKey = parent.GetKeyAt(childIndex - 1);

            // Merge child into left sibling using direct copy (no InsertChild checks)
            leftInternal.MergeFrom(separatorKey, childInternal);

            // Remove separator from parent
            parent.RemoveKey(childIndex - 1);

            await session.WriteAsync(leftInternal);
            await session.WriteAsync(parent);
        }
    }

    private async Task MergeWithRight(InternalNode<TK> parent, TreeNode<TK> child, TreeNode<TK> rightSibling,
        int childIndex)
    {
        if (child.IsLeaf)
        {
            var childLeaf = (LeafNode<TK, TV>)child;
            var rightLeaf = (LeafNode<TK, TV>)rightSibling;

            // Check if merge is possible (total keys must fit in one node)
            var totalKeys = childLeaf.KeyCount + rightLeaf.KeyCount;
            if (totalKeys > childLeaf.Degree)
            {
                // Cannot merge - would exceed capacity
                return;
            }

            // Merge right sibling into child using direct copy (no Insert checks)
            childLeaf.MergeFrom(rightLeaf);

            // Update next pointer
            childLeaf.NextLeaf = rightLeaf.NextLeaf;

            // Remove separator from parent
            parent.RemoveKey(childIndex);

            await session.WriteAsync(childLeaf);
            await session.WriteAsync(parent);
        }
        else
        {
            var childInternal = (InternalNode<TK>)child;
            var rightInternal = (InternalNode<TK>)rightSibling;

            // Check capacity: child keys + separator + right keys must fit
            var totalKeys = childInternal.KeyCount + 1 + rightInternal.KeyCount; // +1 for separator
            if (totalKeys > childInternal.Degree)
            {
                // Cannot merge - would exceed capacity
                return;
            }

            // Get separator key from parent
            var separatorKey = parent.GetKeyAt(childIndex);

            // Merge right sibling into child using direct copy (no InsertChild checks)
            childInternal.MergeFrom(separatorKey, rightInternal);

            // Remove separator from parent
            parent.RemoveKey(childIndex);

            await session.WriteAsync(childInternal);
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