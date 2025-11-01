using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class JsonObjectSerializer(JsonSerializerOptions? options = null) : IObjectSerializer
{
    private readonly JsonSerializerOptions _options = options ?? new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public byte[] Serialize(object obj)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));
        
        var json = JsonSerializer.Serialize(obj, _options);
        return Encoding.UTF8.GetBytes(json);
    }
    
    public T Deserialize<T>(byte[] bytes) where T : class
    {
        if (bytes == null || bytes.Length == 0)
            throw new ArgumentException("Bytes cannot be null or empty", nameof(bytes));
        
        var json = Encoding.UTF8.GetString(bytes);
        return JsonSerializer.Deserialize<T>(json, _options)
               ?? throw new InvalidOperationException("Deserialization returned null");
    }
    
    public int GetSize(object obj)
    {
        // JSON is variable size
        return -1;
    }
    
    public bool CanSerialize(Type type)
    {
        return type is { IsPointer: false, IsByRef: false };
    }
}