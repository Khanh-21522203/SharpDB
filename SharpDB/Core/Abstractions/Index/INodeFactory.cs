using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Core.Abstractions.Index;

public interface INodeFactory<TK, TV>
    where TK : IComparable<TK>
{
    TreeNode<TK> FromBytes(byte[] bytes);
    
    LeafNode<TK, TV> CreateLeaf();
    InternalNode<TK> CreateInternal();
    
    int GetLeafNodeSize();
    int GetInternalNodeSize();
}