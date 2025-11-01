using System.Text;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Engine;

namespace SharpDB.Serialization;

/// <summary>
/// Serializer for composite keys (multi-field primary keys).
/// Fixed-size, deterministic ordering.
/// </summary>
public class CompositeKeySerializer : IObjectSerializer
{
    private readonly List<(string Name, FieldType Type, int Size)> _keyFields;
    private readonly int _totalSize;
    
    public CompositeKeySerializer(List<(string Name, FieldType Type, int Size)> keyFields)
    {
        _keyFields = keyFields ?? throw new ArgumentNullException(nameof(keyFields));
        _totalSize = keyFields.Sum(f => f.Size);
    }
    
    public byte[] Serialize(object obj)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));
        
        var bytes = new byte[_totalSize];
        var offset = 0;
        
        foreach (var (name, type, size) in _keyFields)
        {
            var value = GetFieldValue(obj, name);
            WriteValue(bytes, ref offset, type, size, value);
        }
        
        return bytes;
    }
    
    public T Deserialize<T>(byte[] bytes) where T : class
    {
        if (bytes == null || bytes.Length != _totalSize)
            throw new ArgumentException($"Expected {_totalSize} bytes", nameof(bytes));
        
        var instance = Activator.CreateInstance<T>();
        var offset = 0;
        
        foreach (var (name, type, size) in _keyFields)
        {
            var value = ReadValue(bytes, ref offset, type, size);
            SetFieldValue(instance, name, value);
        }
        
        return instance;
    }
    
    public int GetSize(object obj)
    {
        return _totalSize;
    }
    
    public bool CanSerialize(Type type)
    {
        // Check if type has all required fields
        foreach (var (name, _, _) in _keyFields)
        {
            if (type.GetProperty(name) == null)
                return false;
        }
        return true;
    }
    
    private void WriteValue(byte[] bytes, ref int offset, FieldType type, int size, object? value)
    {
        switch (type)
        {
            case FieldType.Int:
                BitConverter.GetBytes((int)(value ?? 0)).CopyTo(bytes, offset);
                break;
            case FieldType.Long:
                BitConverter.GetBytes((long)(value ?? 0L)).CopyTo(bytes, offset);
                break;
            case FieldType.String:
                var str = value?.ToString() ?? "";
                var strBytes = Encoding.UTF8.GetBytes(str.PadRight(size));
                Array.Copy(strBytes, 0, bytes, offset, size);
                break;
            case FieldType.DateTime:
                var dt = value != null ? (DateTime)value : DateTime.MinValue;
                BitConverter.GetBytes(dt.Ticks).CopyTo(bytes, offset);
                break;
        }
        offset += size;
    }
    
    private object? ReadValue(byte[] bytes, ref int offset, FieldType type, int size)
    {
        object? value = null;
        
        switch (type)
        {
            case FieldType.Int:
                value = BitConverter.ToInt32(bytes, offset);
                break;
            case FieldType.Long:
                value = BitConverter.ToInt64(bytes, offset);
                break;
            case FieldType.String:
                value = Encoding.UTF8.GetString(bytes, offset, size).TrimEnd('\0');
                break;
            case FieldType.DateTime:
                value = new DateTime(BitConverter.ToInt64(bytes, offset));
                break;
        }
        
        offset += size;
        return value;
    }
    
    private object? GetFieldValue(object obj, string fieldName)
    {
        return obj.GetType().GetProperty(fieldName)?.GetValue(obj);
    }
    
    private void SetFieldValue(object obj, string fieldName, object? value)
    {
        obj.GetType().GetProperty(fieldName)?.SetValue(obj, value);
    }
}