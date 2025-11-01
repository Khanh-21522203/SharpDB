using SharpDB.Core.Abstractions.Index;
using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

public class NodeFactory<TK, TV>(
    IIndexBinaryObjectFactory<TK> keyFactory,
    IIndexBinaryObjectFactory<TV> valueFactory,
    int degree)
    where TK : IComparable<TK>
{
    public TreeNode<TK> FromBytes(byte[] bytes)
    {
        var isLeaf = (bytes[0] & TreeNode<TK>.TypeLeafBit) != 0;

        if (isLeaf) return new LeafNode<TK, TV>(bytes, degree, keyFactory, valueFactory);

        return new InternalNode<TK>(bytes, keyFactory, degree);
    }

    public LeafNode<TK, TV> CreateLeaf()
    {
        return new LeafNode<TK, TV>(degree, keyFactory, valueFactory);
    }

    public InternalNode<TK> CreateInternal()
    {
        return new InternalNode<TK>(keyFactory, degree);
    }

    public int GetLeafNodeSize()
    {
        return 1 + (degree - 1) * (keyFactory.Size + valueFactory.Size) + 2 * Pointer.ByteSize;
    }

    public int GetInternalNodeSize()
    {
        return 1 + degree * Pointer.ByteSize + (degree - 1) * keyFactory.Size;
    }
}