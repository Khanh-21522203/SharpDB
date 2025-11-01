using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

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
    
    public LeafNode<TK, Pointer> CreateLeafNode() => _innerFactory.CreateLeafNode();
    
    public InternalNode<TK> CreateInternalNode() => _innerFactory.CreateInternalNode();
    
    public TreeNode<TK> DeserializeNode(byte[] data) => _innerFactory.DeserializeNode(data);
}

public class PointerSerializer : ISerializer<Pointer>
{
    public int Size => Pointer.ByteSize;
    
    public byte[] Serialize(Pointer obj) => obj.ToBytes();
    
    public Pointer Deserialize(byte[] bytes, int offset = 0) => 
        Pointer.FromBytes(bytes, offset);
}