using SharpDB.Core.Abstractions.Index;
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
public class LeafNode<TK, TV> : TreeNode<TK> where TK : IComparable<TK>
{
    private readonly IIndexBinaryObjectFactory<TK> _keyFactory;
    private readonly IIndexBinaryObjectFactory<TV> _valueFactory;
    private readonly int _degree;
    
    public override bool IsLeaf => true;
    
    // Constructor for deserialization
    public LeafNode(
        byte[] data,
        int degree,
        IIndexBinaryObjectFactory<TK> keyFactory,
        IIndexBinaryObjectFactory<TV> valueFactory)
        : base(data)
    {
        _degree = degree;
        _keyFactory = keyFactory;
        _valueFactory = valueFactory;
    }
    // Constructor for new node
    public LeafNode(
        int degree, 
        IIndexBinaryObjectFactory<TK> keyFactory,
        IIndexBinaryObjectFactory<TV> valueFactory)
        : base(new byte[CalculateSize(keyFactory.Size, valueFactory.Size, degree)])
    {
        _degree = degree;
        _valueFactory = valueFactory;
        _keyFactory = keyFactory;
    }
    
    // Header + KeyValues + Next pointer + Prev pointer
    private static int CalculateSize(int keySize, int valueSize, int degree)
        => 1 + (degree - 1) * (keySize + valueSize) + 2 * Pointer.ByteSize;

    public List<KeyValue<TK, TV>> GetKeyValues()
    {
        var result = new List<KeyValue<TK, TV>>();
        var kvSize = _keyFactory.Size + _valueFactory.Size;

        for (var i = 0; i < _degree - 1; i++)
        {
            var offset = 1 + i * kvSize;
            if (IsEmpty(offset, _keyFactory.Size))
                break;

            var keyBinary = _keyFactory.Create(Data, offset);
            var key = keyBinary.AsObject();
            
            var  valueBinary = _valueFactory.Create(Data, offset);
            var value = valueBinary.AsObject();
            
            result.Add(new KeyValue<TK, TV>(key, value));
        }
        return result;
    }

    public void SetKeyValues(List<KeyValue<TK, TV>> keyValues)
    {
        var kvSize = _keyFactory.Size + _valueFactory.Size;
        
        // Write key-values
        for (var i = 0; i < keyValues.Count; i++)
        {
            var offset = 1 + (i * kvSize);
            
            var keyBinary = _keyFactory.Create(keyValues[i].Key);
            var valueBinary = _valueFactory.Create(keyValues[i].Value);
            
            Array.Copy(keyBinary.GetBytes(), 0, Data, offset, _keyFactory.Size);
            Array.Copy(valueBinary.GetBytes(), 0, Data, offset + _keyFactory.Size, _valueFactory.Size);
        }
        // Clear remaining slots
        for (var i = keyValues.Count; i < _degree - 1; i++)
        {
            var offset = 1 + (i * kvSize);
            Array.Clear(Data, offset, kvSize);
        }
        
        MarkModified();
    }

    public int AddKeyValue(TK key, TV value)
    {
        var keyValues = GetKeyValues();
        var newKv = new KeyValue<TK, TV>(key, value);
        
        var index = keyValues.BinarySearch(newKv);
        if (index >= 0)
            throw new InvalidOperationException($"Key {key} already exists");
        
        var insertIndex = ~index;
        keyValues.Insert(insertIndex, newKv);
        
        SetKeyValues(keyValues);
        
        return insertIndex;
    }
    
    public bool RemoveKeyValue(TK key)
    {
        var keyValues = GetKeyValues();
        var searchKv = new KeyValue<TK, TV>(key, default!);
        
        var index = keyValues.BinarySearch(searchKv);
        if (index < 0)
            return false;
        
        keyValues.RemoveAt(index);
        SetKeyValues(keyValues);
        
        return true;
    }
    
    public bool UpdateKeyValue(TK key, TV newValue)
    {
        var keyValues = GetKeyValues();
        var searchKv = new KeyValue<TK, TV>(key, default!);
        
        var index = keyValues.BinarySearch(searchKv);
        if (index < 0)
            return false;
        
        keyValues[index] = new KeyValue<TK, TV>(key, newValue);
        SetKeyValues(keyValues);
        
        return true;
    }
    
    public bool TryGetValue(TK key, out TV? value)
    {
        var keyValues = GetKeyValues();
        var searchKv = new KeyValue<TK, TV>(key, default!);
        
        var index = keyValues.BinarySearch(searchKv);
        if (index < 0)
        {
            value = default;
            return false;
        }
        
        value = keyValues[index].Value;
        return true;
    }
    
    public bool IsFull() => GetKeyValues().Count >= _degree - 1;
    
    public List<KeyValue<TK, TV>> AddAndSplit(TK key, TV value)
    {
        var keyValues = GetKeyValues();
        var newKv = new KeyValue<TK, TV>(key, value);
        
        var index = keyValues.BinarySearch(newKv);
        var insertIndex = index >= 0 ? index : ~index;
        keyValues.Insert(insertIndex, newKv);
        
        var mid = (_degree - 1) / 2;
        
        var leftHalf = keyValues.Take(mid + 1).ToList();
        SetKeyValues(leftHalf);
        
        return keyValues.Skip(mid + 1).ToList();
    }
    
    public Pointer? GetNextSibling()
    {
        var offset = Data.Length - (2 * Pointer.ByteSize);
        
        if (IsEmpty(offset, Pointer.ByteSize))
            return null;
        
        return Pointer.FromBytes(Data, offset);
    }
    
    
    public void SetNextSibling(Pointer? pointer)
    {
        var offset = Data.Length - (2 * Pointer.ByteSize);
        
        if (pointer == null)
        {
            Array.Clear(Data, offset, Pointer.ByteSize);
        }
        else
        {
            pointer.FillBytes(Data, offset);
        }
        
        MarkModified();
    }
    
    public Pointer? GetPreviousSibling()
    {
        var offset = Data.Length - Pointer.ByteSize;
        
        if (IsEmpty(offset, Pointer.ByteSize))
            return null;
        
        return Pointer.FromBytes(Data, offset);
    }
    
    public void SetPreviousSibling(Pointer? pointer)
    {
        var offset = Data.Length - Pointer.ByteSize;
        
        if (pointer == null)
        {
            Array.Clear(Data, offset, Pointer.ByteSize);
        }
        else
        {
            pointer.FillBytes(Data, offset);
        }
        
        MarkModified();
    }
    
    public override string ToString()
    {
        var kvs = GetKeyValues();
        return $"LeafNode[Count={kvs.Count}, Root={IsRoot}]";
    }
}