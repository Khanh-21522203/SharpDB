using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

/// <summary>
/// Internal node stores keys and child pointers for routing.
/// Structure: [Header(6)] [Keys(degree*keySize)] [Children((degree+1)*13)]
/// </summary>
public class InternalNode<TK> : TreeNode<TK>
    where TK : IComparable<TK>
{
    private readonly int _keysOffset;
    private readonly int _childrenOffset;
    
    public override bool IsLeaf => false;
    
    public InternalNode(byte[] data, ISerializer<TK> keySerializer, int degree)
        : base(data, keySerializer, degree)
    {
        _keysOffset = 6;
        _childrenOffset = _keysOffset + (degree * _keySerializer.Size);
        
        // Set internal bit
        _data[0] |= TypeInternalBit;
    }
    
    public Pointer GetChild(int index)
    {
        if (index < 0 || index > KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var offset = _childrenOffset + (index * Pointer.ByteSize);
        return Pointer.FromBytes(_data, offset);
    }
    
    public void SetChild(int index, Pointer pointer)
    {
        if (index < 0 || index > KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var offset = _childrenOffset + (index * Pointer.ByteSize);
        pointer.ToBytes().CopyTo(_data, offset);
        MarkModified();
    }
    
    /// <summary>
    /// Find child pointer for given key.
    /// </summary>
    public Pointer FindChild(TK key)
    {
        int index = FindKeyIndex(key);
        
        // If key is found or should go left, use index
        // Otherwise use index (which points to right child)
        if (index < KeyCount && key.CompareTo(GetKeyAt(index)) >= 0)
            index++;
        
        return GetChild(index);
    }
    
    public void InsertChild(TK key, Pointer childPointer)
    {
        if (IsFull())
            throw new InvalidOperationException("Node is full");
        
        int insertIndex = FindKeyIndex(key);
        
        // Shift keys and children
        if (insertIndex < KeyCount)
        {
            ShiftRight(insertIndex);
        }
        
        // Insert key and child
        SetKeyAt(insertIndex, key);
        SetChild(insertIndex + 1, childPointer);
        KeyCount++;
    }
    
    public TK[] SplitAndGetKeys(out Pointer[] rightChildren)
    {
        var midPoint = KeyCount / 2;
        var rightCount = KeyCount - midPoint - 1; // -1 because mid key goes up
        
        var rightKeys = new TK[rightCount];
        rightChildren = new Pointer[rightCount + 1];
        
        // Copy right keys (excluding middle)
        for (var i = 0; i < rightCount; i++)
        {
            rightKeys[i] = GetKeyAt(midPoint + 1 + i);
        }
        
        // Copy right children
        for (var i = 0; i <= rightCount; i++)
        {
            rightChildren[i] = GetChild(midPoint + 1 + i);
        }
        
        // Middle key to promote
        var middleKey = GetKeyAt(midPoint);
        
        // Truncate this node
        KeyCount = midPoint;
        
        return rightKeys;
    }
    
    public override bool IsFull() => KeyCount >= _degree;
    
    public override bool IsMinimum() => KeyCount < (_degree + 1) / 2;
    
    public override TK GetKeyAt(int index)
    {
        var offset = _keysOffset + (index * _keySerializer.Size);
        return GetKey(index, offset);
    }
    
    public void SetKeyAt(int index, TK key)
    {
        var offset = _keysOffset + (index * _keySerializer.Size);
        SetKey(index, key, offset);
    }

    public bool RemoveKey(int index)
    {
        if (index < 0 || index >= KeyCount)
            return false;
        
        ShiftLeft(index + 1);
        KeyCount--;
        
        return true;
    }

    private void ShiftRight(int startIndex)
    {
        var keySize = _keySerializer.Size;
        
        for (var i = KeyCount - 1; i >= startIndex; i--)
        {
            // Shift key
            var srcKeyOffset = _keysOffset + (i * keySize);
            var dstKeyOffset = _keysOffset + ((i + 1) * keySize);
            Array.Copy(_data, srcKeyOffset, _data, dstKeyOffset, keySize);
            
            // Shift child pointer
            var srcChildOffset = _childrenOffset + ((i + 1) * Pointer.ByteSize);
            var dstChildOffset = _childrenOffset + ((i + 2) * Pointer.ByteSize);
            Array.Copy(_data, srcChildOffset, _data, dstChildOffset, Pointer.ByteSize);
        }
    }
    
    private void ShiftLeft(int startIndex)
    {
        var keySize = _keySerializer.Size;
        
        for (var i = startIndex; i < KeyCount; i++)
        {
            // Shift key
            var srcKeyOffset = _keysOffset + (i * keySize);
            var dstKeyOffset = _keysOffset + ((i - 1) * keySize);
            Array.Copy(_data, srcKeyOffset, _data, dstKeyOffset, keySize);
            
            // Shift child pointer
            var srcChildOffset = _childrenOffset + (i * Pointer.ByteSize);
            var dstChildOffset = _childrenOffset + ((i - 1) * Pointer.ByteSize);
            Array.Copy(_data, srcChildOffset, _data, dstChildOffset, Pointer.ByteSize);
        }
    }
}