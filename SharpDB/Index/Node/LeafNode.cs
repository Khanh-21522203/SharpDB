using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

// LeafNode Binary Structure:
//
//     ┌─────────┬───────────────┬───────────────┬─────┬─────────┬─────────┐
//     │ Header  │  KeyValue 1   │  KeyValue 2   │ ... │  Next   │  Prev   │
//     │  1 byte │  K + V bytes  │  K + V bytes  │     │ 13B     │  13B    │
//     └─────────┴───────────────┴───────────────┴─────┴─────────┴─────────┘
//        byte 0   bytes 1...      ...                    end-26   end-13
//
// KeyValue entry:
//     ┌─────────┬─────────┐
//     │   Key   │  Value  │
//     │ KSize   │ VSize   │
//     └─────────┴─────────┘
//
// Total size: 1 + (degree-1) × (KSize + VSize) + 26
/// <summary>
/// Leaf node stores actual key-value pairs.
/// Structure: [Header(6)] [Keys(degree*keySize)] [Values(degree*valueSize)] [NextPointer(13)]
/// </summary>
public class LeafNode<TK, TV> : TreeNode<TK>
    where TK : IComparable<TK>
{
    private readonly ISerializer<TV> _valueSerializer;
    private readonly int _keysOffset;
    private readonly int _valuesOffset;
    private readonly int _nextPointerOffset;
    
    public override bool IsLeaf => true;
    
    /// <summary>
    /// Create new leaf node.
    /// </summary>
    public LeafNode(byte[] data, ISerializer<TK> keySerializer, ISerializer<TV> valueSerializer, int degree)
        : base(data, keySerializer, degree)
    {
        _valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
        
        // Calculate offsets
        // Header: Type(1) + KeyCount(4) + Reserved(1) = 6 bytes
        _keysOffset = 6;
        _valuesOffset = _keysOffset + (degree * _keySerializer.Size);
        _nextPointerOffset = _valuesOffset + (degree * _valueSerializer.Size);
        
        // Set leaf bit
        _data[0] |= TypeLeafBit;
    }
    
    public Pointer? NextLeaf
    {
        get
        {
            var hasNext = _data[_nextPointerOffset];
            if (hasNext == 0)
                return null;
            
            return Pointer.FromBytes(_data, _nextPointerOffset + 1);
        }
        set
        {
            if (value == null)
            {
                _data[_nextPointerOffset] = 0;
            }
            else
            {
                _data[_nextPointerOffset] = 1;
                value.Value.ToBytes().CopyTo(_data, _nextPointerOffset + 1);
            }
            MarkModified();
        }
    }
    
    public void Insert(TK key, TV value)
    {
        if (IsFull())
            throw new InvalidOperationException("Node is full");
        
        int insertIndex = FindKeyIndex(key);
        
        // Shift keys and values to make room
        if (insertIndex < KeyCount)
        {
            ShiftRight(insertIndex);
        }
        
        // Insert key and value
        SetKeyAt(insertIndex, key);
        SetValueAt(insertIndex, value);
        KeyCount++;
    }
    
    public bool TryGetValue(TK key, out TV? value)
    {
        var index = FindKeyIndex(key);
        
        if (index < KeyCount && GetKeyAt(index).CompareTo(key) == 0)
        {
            value = GetValueAt(index);
            return true;
        }
        
        value = default;
        return false;
    }
    
    public bool Remove(TK key)
    {
        var index = FindKeyIndex(key);
        
        if (index >= KeyCount || GetKeyAt(index).CompareTo(key) != 0)
            return false; // Key not found
        
        // Shift left to remove
        ShiftLeft(index + 1);
        KeyCount--;
        
        return true;
    }
    
    public (TK[] Keys, TV[] Values) Split()
    {
        var midPoint = KeyCount / 2;
        var rightCount = KeyCount - midPoint;
        
        var rightKeys = new TK[rightCount];
        var rightValues = new TV[rightCount];
        
        // Copy right half
        for (var i = 0; i < rightCount; i++)
        {
            rightKeys[i] = GetKeyAt(midPoint + i);
            rightValues[i] = GetValueAt(midPoint + i);
        }
        
        // Truncate this node
        KeyCount = midPoint;
        
        return (rightKeys, rightValues);
    }
    
    public override bool IsFull() => KeyCount >= _degree;
    
    public override bool IsMinimum() => KeyCount < (_degree + 1) / 2;
    
    public override TK GetKeyAt(int index)
    {
        var offset = _keysOffset + (index * _keySerializer.Size);
        return GetKey(index, offset);
    }
    
    private void SetKeyAt(int index, TK key)
    {
        var offset = _keysOffset + (index * _keySerializer.Size);
        SetKey(index, key, offset);
    }
    
    private TV GetValueAt(int index)
    {
        var offset = _valuesOffset + (index * _valueSerializer.Size);
        return _valueSerializer.Deserialize(_data, offset);
    }
    
    private void SetValueAt(int index, TV value)
    {
        var offset = _valuesOffset + (index * _valueSerializer.Size);
        var valueBytes = _valueSerializer.Serialize(value);
        Array.Copy(valueBytes, 0, _data, offset, valueBytes.Length);
        MarkModified();
    }
    
    private void ShiftRight(int startIndex)
    {
        var keySize = _keySerializer.Size;
        var valueSize = _valueSerializer.Size;
        
        for (var i = KeyCount - 1; i >= startIndex; i--)
        {
            // Shift key
            var srcKeyOffset = _keysOffset + (i * keySize);
            var dstKeyOffset = _keysOffset + ((i + 1) * keySize);
            Array.Copy(_data, srcKeyOffset, _data, dstKeyOffset, keySize);
            
            // Shift value
            var srcValueOffset = _valuesOffset + (i * valueSize);
            var dstValueOffset = _valuesOffset + ((i + 1) * valueSize);
            Array.Copy(_data, srcValueOffset, _data, dstValueOffset, valueSize);
        }
    }
    
    private void ShiftLeft(int startIndex)
    {
        var keySize = _keySerializer.Size;
        var valueSize = _valueSerializer.Size;
        
        for (var i = startIndex; i < KeyCount; i++)
        {
            // Shift key
            var srcKeyOffset = _keysOffset + (i * keySize);
            var dstKeyOffset = _keysOffset + ((i - 1) * keySize);
            Array.Copy(_data, srcKeyOffset, _data, dstKeyOffset, keySize);
            
            // Shift value
            var srcValueOffset = _valuesOffset + (i * valueSize);
            var dstValueOffset = _valuesOffset + ((i - 1) * valueSize);
            Array.Copy(_data, srcValueOffset, _data, dstValueOffset, valueSize);
        }
    }
}