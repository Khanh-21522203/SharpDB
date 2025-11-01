using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.DataStructures;

public class BinaryList<TV>
    where TV : struct, IComparable<TV>
{
    public const int MetaSize = sizeof(int);
    
    private readonly ISerializer<TV> _serializer;
    private byte[] _data;
    
    public int Count
    {
        get => BitConverter.ToInt32(_data, 0);
        private set => BitConverter.GetBytes(value).CopyTo(_data, 0);
    }
    
    public BinaryList(ISerializer<TV> serializer, byte[] data)
    {
        _serializer = serializer;
        _data = data;
    }
    
    public BinaryList(ISerializer<TV> serializer, int initialCapacity)
    {
        _serializer = serializer;
        _data = new byte[MetaSize + initialCapacity * _serializer.Size];
        Count = 0;
    }
    
    public void Initialize()
    {
        Count = 0;
    }
    
    public void Add(TV value)
    {
        int capacity = (_data.Length - MetaSize) / _serializer.Size;
        
        if (Count >= capacity)
        {
            // Grow array
            int newCapacity = capacity * 2;
            var newData = new byte[MetaSize + newCapacity * _serializer.Size];
            Array.Copy(_data, newData, _data.Length);
            _data = newData;
        }
        
        // Add value
        int offset = MetaSize + Count * _serializer.Size;
        var valueBytes = _serializer.Serialize(value);
        Array.Copy(valueBytes, 0, _data, offset, valueBytes.Length);
        
        Count++;
    }
    
    public bool Remove(TV value)
    {
        int index = IndexOf(value);
        if (index < 0) return false;
        
        // Shift elements
        int offset = MetaSize + index * _serializer.Size;
        int bytesToMove = (Count - index - 1) * _serializer.Size;
        
        if (bytesToMove > 0)
        {
            Array.Copy(_data, offset + _serializer.Size, 
                      _data, offset, bytesToMove);
        }
        
        Count--;
        return true;
    }
    
    public int IndexOf(TV value)
    {
        for (int i = 0; i < Count; i++)
        {
            int offset = MetaSize + i * _serializer.Size;
            var item = _serializer.Deserialize(_data, offset);
            
            if (item.CompareTo(value) == 0)
                return i;
        }
        
        return -1;
    }
    
    public IEnumerable<TV> GetItems()
    {
        for (int i = 0; i < Count; i++)
        {
            int offset = MetaSize + i * _serializer.Size;
            yield return _serializer.Deserialize(_data, offset);
        }
    }
    
    public byte[] Data => _data;
}