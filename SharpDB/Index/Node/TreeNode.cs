using SharpDB.Core.Abstractions.Serialization;
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

/// <summary>
///     Abstract base class for B+ tree nodes.
///     Stores node data in fixed-size byte array for disk persistence.
/// </summary>
public abstract class TreeNode<TK>(byte[] data, ISerializer<TK> keySerializer, int degree)
    where TK : IComparable<TK>
{
    public const byte TypeLeafBit = 0x02;
    public const byte TypeInternalBit = 0x01;
    public const byte RootBit = 0x04;

    protected readonly byte[] _data = data;
    protected readonly int _degree = degree;
    protected readonly ISerializer<TK> _keySerializer = keySerializer;

    //TODO: Need to consider as Pointer?
    public Pointer Pointer { get; set; }
    public bool Modified { get; private set; }

    public abstract bool IsLeaf { get; }

    public bool IsRoot => (_data[0] & RootBit) != 0;

    public int KeyCount
    {
        get => BitConverter.ToInt32(_data, 1);
        protected set
        {
            BitConverter.GetBytes(value).CopyTo(_data, 1);
            MarkModified();
        }
    }

    public void SetAsRoot()
    {
        _data[0] |= RootBit;
        MarkModified();
    }

    public void UnsetAsRoot()
    {
        _data[0] &= unchecked((byte)~RootBit);
        MarkModified();
    }

    public byte[] ToBytes()
    {
        return _data;
    }

    protected void MarkModified()
    {
        Modified = true;
    }

    public void ClearModified()
    {
        Modified = false;
    }

    protected void SetKey(int index, TK key, int offset)
    {
        if (index < 0 || index >= _degree)
            throw new ArgumentOutOfRangeException(nameof(index));

        var keyBytes = _keySerializer.Serialize(key);
        Array.Copy(keyBytes, 0, _data, offset, keyBytes.Length);
        MarkModified();
    }

    protected TK GetKey(int index, int offset)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        return _keySerializer.Deserialize(_data, offset);
    }

    public int FindKeyIndex(TK key)
    {
        var left = 0;
        var right = KeyCount - 1;

        while (left <= right)
        {
            var mid = left + (right - left) / 2;
            var midKey = GetKeyAt(mid);
            var cmp = key.CompareTo(midKey);

            if (cmp == 0)
                return mid;
            if (cmp < 0)
                right = mid - 1;
            else
                left = mid + 1;
        }

        return left;
    }

    public abstract TK GetKeyAt(int index);

    public abstract bool IsFull();
    public abstract bool IsMinimum();
}