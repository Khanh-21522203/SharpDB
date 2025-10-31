using SharpDB.Core.Abstractions.Index;
using SharpDB.DataStructures;

namespace SharpDB.Index.Node;

/// <summary>
/// Internal node in B+ Tree.
/// Contains keys for routing and pointers to child nodes.
/// </summary>
public class InternalNode<TK> : TreeNode<TK>
    where TK : IComparable<TK>
{
    private readonly IIndexBinaryObjectFactory<TK> _keyFactory;
    private readonly int _degree;
    
    public override bool IsLeaf => false;
    
    // Constructor for new node
    public InternalNode(
        IIndexBinaryObjectFactory<TK> keyFactory,
        int degree)
        : base(new byte[CalculateSize(keyFactory.Size, degree)])
    {
        _keyFactory = keyFactory;
        _degree = degree;
        Data[0] |= TypeInternalBit;
    }
    
    // Constructor for deserialization
    public InternalNode(
        byte[] data,
        IIndexBinaryObjectFactory<TK> keyFactory,
        int degree)
        : base(data)
    {
        _keyFactory = keyFactory;
        _degree = degree;
    }
    
    // Header + (degree children) + (degree-1 keys)
    private static int CalculateSize(int keySize, int degree)
        => 1 + (degree * Pointer.ByteSize) + ((degree - 1) * keySize);
    
    public List<TK> GetKeys()
    {
        var result = new List<TK>();
        
        for (var i = 0; i < _degree - 1; i++)
        {
            int offset = GetKeyOffset(i);
            
            if (IsEmpty(offset, _keyFactory.Size))
                break;
            
            var keyBinary = _keyFactory.Create(Data, offset);
            result.Add(keyBinary.AsObject());
        }
        return result;
    }
    
    public void SetKeys(List<TK> keys)
    {
        for (var i = 0; i < keys.Count; i++)
        {
            SetKey(i, keys[i]);
        }
        
        for (var i = keys.Count; i < _degree - 1; i++)
        {
            var offset = GetKeyOffset(i);
            Array.Clear(Data, offset, _keyFactory.Size);
        }
        
        MarkModified();
    }
    
    public void SetKey(int index, TK key)
    {
        if (index < 0 || index >= _degree - 1)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var offset = GetKeyOffset(index);
        var keyBinary = _keyFactory.Create(key);
        Array.Copy(keyBinary.GetBytes(), 0, Data, offset, _keyFactory.Size);
        
        MarkModified();
    }
    
    public TK GetKey(int index)
    {
        if (index < 0 || index >= _degree - 1)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var offset = GetKeyOffset(index);
        var keyBinary = _keyFactory.Create(Data, offset);
        return keyBinary.AsObject();
    }
    
    public Pointer GetChildAt(int index)
    {
        if (index < 0 || index >= _degree)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var offset = GetChildOffset(index);
        return Pointer.FromBytes(Data, offset);
    }
    
    public void SetChildAt(int index, Pointer pointer)
    {
        if (index < 0 || index >= _degree)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var offset = GetChildOffset(index);
        pointer.FillBytes(Data, offset);
        
        MarkModified();
    }
    
    public int FindChildIndex(TK key)
    {
        var keys = GetKeys();
        
        for (var i = 0; i < keys.Count; i++)
        {
            if (key.CompareTo(keys[i]) < 0)
                return i;
        }
        
        return keys.Count;
    }
    
    public void InsertKey(int index, TK key, Pointer rightChild)
    {
        var keys = GetKeys();
        
        if (index < 0 || index > keys.Count)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        keys.Insert(index, key);
        SetKeys(keys);
        
        for (var i = keys.Count; i > index + 1; i--)
        {
            var child = GetChildAt(i - 1);
            SetChildAt(i, child);
        }
        
        SetChildAt(index + 1, rightChild);
    }
    
    public bool IsFull() => GetKeys().Count >= _degree - 1;
    
    // Layout: [Header:1][Child0:13][Key0:KSize][Child1:13][Key1:KSize]...
    private int GetKeyOffset(int keyIndex)
        => 1 + Pointer.ByteSize + (keyIndex * (Pointer.ByteSize + _keyFactory.Size));
    
    // Layout: [Header:1][Child0:13][Key0:KSize][Child1:13][Key1:KSize]...
    private int GetChildOffset(int childIndex)
        => 1 + (childIndex * (Pointer.ByteSize + _keyFactory.Size));
    
    public override string ToString()
    {
        var keys = GetKeys();
        return $"InternalNode[KeyCount={keys.Count}, Root={IsRoot}]";
    }
}