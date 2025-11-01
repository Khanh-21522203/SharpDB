using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

public class BPlusTreeNodeFactory<TK, TV>(
    ISerializer<TK> keySerializer,
    ISerializer<TV> valueSerializer,
    int degree)
    : INodeFactory<TK, TV>
    where TK : IComparable<TK>
{
    public LeafNode<TK, TV> CreateLeafNode()
    {
        var nodeSize = CalculateLeafNodeSize();
        var data = new byte[nodeSize];
        return new LeafNode<TK, TV>(data, keySerializer, valueSerializer, degree);
    }

    public InternalNode<TK> CreateInternalNode()
    {
        var nodeSize = CalculateInternalNodeSize();
        var data = new byte[nodeSize];
        return new InternalNode<TK>(data, keySerializer, degree);
    }

    public TreeNode<TK> DeserializeNode(byte[] data)
    {
        var typeFlags = data[0];
        var isLeaf = (typeFlags & TreeNode<TK>.TypeLeafBit) != 0;

        if (isLeaf)
            return new LeafNode<TK, TV>(data, keySerializer, valueSerializer, degree);
        return new InternalNode<TK>(data, keySerializer, degree);
    }

    private int CalculateLeafNodeSize()
    {
        // Header(6) + Keys(degree*keySize) + Values(degree*valueSize) + NextPointer(14)
        return 6 + degree * keySerializer.Size + degree * valueSerializer.Size + 14;
    }

    private int CalculateInternalNodeSize()
    {
        // Header(6) + Keys(degree*keySize) + Children((degree+1)*13)
        return 6 + degree * keySerializer.Size + (degree + 1) * Pointer.ByteSize;
    }
}