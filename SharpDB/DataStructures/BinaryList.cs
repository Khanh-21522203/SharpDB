using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.DataStructures;

/// <summary>
/// Stores a list of values as a single serialized byte array.
/// Used for duplicate index values.
/// </summary>
public class BinaryList<TV> where TV : IComparable<TV>
{
    private readonly List<TV> _values = [];
    private readonly ISerializer<TV> _serializer;
    
    public BinaryList(ISerializer<TV> serializer)
    {
        _serializer = serializer;
    }
    
    public BinaryList(ISerializer<TV> serializer, IEnumerable<TV> values)
    {
        _serializer = serializer;
        _values = new List<TV>(values);
        _values.Sort();
    }
    
    public void Add(TV value)
    {
        _values.Add(value);
        _values.Sort();
    }
    
    public bool Remove(TV value)
    {
        return _values.Remove(value);
    }
    
    public void Clear()
    {
        _values.Clear();
    }
    
    public int Count => _values.Count;
    
    public TV this[int index] => _values[index];
    
    public List<TV> ToList() => [.._values];
    
    public byte[] ToBytes()
    {
        var buffer = new byte[sizeof(int) + _values.Count * _serializer.Size];
        BitConverter.GetBytes(_values.Count).CopyTo(buffer, 0);
        
        var offset = sizeof(int);
        foreach (var value in _values)
        {
            _serializer.Serialize(value).CopyTo(buffer, offset);
            offset += _serializer.Size;
        }
        
        return buffer;
    }
    
    public static BinaryList<TV> FromBytes(byte[] bytes, ISerializer<TV> serializer, int offset = 0)
    {
        var count = BitConverter.ToInt32(bytes, offset);
        offset += sizeof(int);
        
        var values = new List<TV>();
        for (var i = 0; i < count; i++)
        {
            var value = serializer.Deserialize(bytes, offset);
            values.Add(value);
            offset += serializer.Size;
        }
        
        return new BinaryList<TV>(serializer, values);
    }
}