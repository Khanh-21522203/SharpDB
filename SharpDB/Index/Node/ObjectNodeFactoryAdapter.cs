using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.Node;

/// <summary>
/// Adapts INodeFactory&lt;TK, TV&gt; to INodeFactory&lt;TK, object&gt; for type erasure.
/// Used when we need to pass a typed factory through a generic interface that expects object values.
/// </summary>
/// <remarks>
/// This adapter is necessary because C# doesn't support covariance on non-readonly generic interfaces.
/// We can't directly cast INodeFactory&lt;TK, TV&gt; to INodeFactory&lt;TK, object&gt;.
/// </remarks>
internal class ObjectNodeFactoryAdapter<TK, TV>(
    INodeFactory<TK, TV> innerFactory,
    bool allowCreateInternalNode = false) 
    : INodeFactory<TK, object>
    where TK : IComparable<TK>
{
    /// <summary>
    /// Creating leaf nodes is not supported because the value types don't match.
    /// Leaf nodes store actual values (TV), but this adapter presents them as object.
    /// </summary>
    public LeafNode<TK, object> CreateLeafNode()
    {
        throw new NotSupportedException(
            "ObjectNodeFactoryAdapter cannot create leaf nodes due to value type mismatch. " +
            "Use DeserializeNode instead.");
    }

    /// <summary>
    /// Creates internal nodes if allowed.
    /// Internal nodes only store keys (no values), so they can be safely created.
    /// </summary>
    public InternalNode<TK> CreateInternalNode()
    {
        if (!allowCreateInternalNode)
        {
            throw new NotSupportedException(
                "This adapter is configured to not create internal nodes. " +
                "Use DeserializeNode instead.");
        }

        return innerFactory.CreateInternalNode();
    }

    /// <summary>
    /// Deserializes nodes from bytes - this always works regardless of value type.
    /// </summary>
    public TreeNode<TK> DeserializeNode(byte[] data)
    {
        return innerFactory.DeserializeNode(data);
    }
}
