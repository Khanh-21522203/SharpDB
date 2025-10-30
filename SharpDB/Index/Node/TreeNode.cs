using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

// Byte 0 - Node Type & Flags:
//
// Bit 7  6  5  4  3  2  1  0
// ┌──┬──┬──┬──┬──┬──┬──┬──┐
// │  │  │  │  │ R│ L│ I│  │
// └──┴──┴──┴──┴──┴──┴──┴──┘
//             │  │  └─── Internal bit (0x01)
//             │  └────── Leaf bit (0x02)
//             └───────── Root bit (0x04)

public abstract class TreeNode<TK>(byte[] data) where TK : IComparable<TK>
{   
    public const byte TypeLeafBit = 0x02;
    public const byte TypeInternalBit = 0x01;
    public const byte RootBit = 0x04;

    protected readonly byte[] Data = data;
    public bool Modified { get; private set; }
    public Pointer? Pointer { get; set; }
    
    public abstract bool IsLeaf { get; }
    public bool IsRoot => (Data[0] & RootBit) != 0;
    
    public void SetAsRoot()
    {
        Data[0] |= RootBit;
        MarkModified();
    }
    
    public void UnsetAsRoot()
    {
        Data[0] &= unchecked((byte)~RootBit);
        MarkModified();
    }
    
    public byte[] ToBytes() => Data;
    
    protected void MarkModified() => Modified = true;
    public void ClearModified() => Modified = false;
    
    // Helper to check if range is empty (all zeros)
    protected bool IsEmpty(int offset, int length)
    {
        for (var i = 0; i < length; i++)
        {
            if (Data[offset + i] != 0)
                return false;
        }
        return true;
    }
    
    public override string ToString()
    {
        return $"{GetType().Name}[Root={IsRoot}, Modified={Modified}]";
    }
}