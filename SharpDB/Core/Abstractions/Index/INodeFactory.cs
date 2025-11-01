using SharpDB.Index.Node;

namespace SharpDB.Core.Abstractions.Index;

public interface INodeFactory<TK, TV> where TK : IComparable<TK>
{
    LeafNode<TK, TV> CreateLeafNode();
    InternalNode<TK> CreateInternalNode();
    TreeNode<TK> DeserializeNode(byte[] data);
}