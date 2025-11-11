using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;
using SharpDB.Serialization;

namespace SharpDB.Index.Node;

public class ClusterNodeFactory<TK> : INodeFactory<TK, Pointer>
    where TK : IComparable<TK>
{
    private readonly BPlusTreeNodeFactory<TK, Pointer> _innerFactory;

    public ClusterNodeFactory(ISerializer<TK> keySerializer, int degree)
    {
        var pointerSerializer = new PointerSerializer();
        _innerFactory = new BPlusTreeNodeFactory<TK, Pointer>(
            keySerializer,
            pointerSerializer,
            degree
        );
    }

    public LeafNode<TK, Pointer> CreateLeafNode()
    {
        return _innerFactory.CreateLeafNode();
    }

    public InternalNode<TK> CreateInternalNode()
    {
        return _innerFactory.CreateInternalNode();
    }

    public TreeNode<TK> DeserializeNode(byte[] data)
    {
        return _innerFactory.DeserializeNode(data);
    }
}