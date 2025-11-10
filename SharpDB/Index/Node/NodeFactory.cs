using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

public class NodeFactory<TK, TV>(
    ISerializer<TK> keySerializer,
    ISerializer<TV> valueSerializer,
    IIndexBinaryObjectFactory<TK> keyFactory,
    IIndexBinaryObjectFactory<TV> valueFactory,
    int degree)
    where TK : IComparable<TK>
{
    public TreeNode<TK> FromBytes(byte[] bytes)
    {
        var isLeaf = (bytes[0] & TreeNode<TK>.TypeLeafBit) != 0;

        if (isLeaf) return new LeafNode<TK, TV>(bytes, keySerializer, valueSerializer, degree);

        return new InternalNode<TK>(bytes, keySerializer, degree);
    }

    public LeafNode<TK, TV> CreateLeaf()
    {
        var nodeSize = GetLeafNodeSize();
        var data = new byte[nodeSize];
        return new LeafNode<TK, TV>(data, keySerializer, valueSerializer, degree);
    }

    public InternalNode<TK> CreateInternal()
    {
        var nodeSize = GetInternalNodeSize();
        var data = new byte[nodeSize];
        return new InternalNode<TK>(data, keySerializer, degree);
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