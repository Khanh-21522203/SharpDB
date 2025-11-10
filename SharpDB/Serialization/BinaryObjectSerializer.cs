using System.Text;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Engine;

namespace SharpDB.Serialization;

/// <summary>
///     Compact binary serializer for performance-critical scenarios.
///     Fixed schema, fast, but requires version management.
/// </summary>
public class BinaryObjectSerializer : IObjectSerializer
{
    private readonly Schema _schema;

    public BinaryObjectSerializer(Schema schema)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _schema.Validate();
    }

    public byte[] Serialize(object obj)
    {
        if (obj == null)
            throw new ArgumentNullException(nameof(obj));

        var type = obj.GetType();
        var size = CalculateSize();
        var bytes = new byte[size];
        var offset = 0;

        // Write schema version
        bytes[offset++] = (byte)_schema.Version;

        // Write each field
        foreach (var field in _schema.Fields)
        {
            var value = GetFieldValue(obj, field.Name);
            WriteField(bytes, ref offset, field, value);
        }

        return bytes;
    }

    public T Deserialize<T>(byte[] bytes) where T : class
    {
        if (bytes == null || bytes.Length == 0)
            throw new ArgumentException("Bytes cannot be null or empty", nameof(bytes));

        var offset = 0;

        // Read schema version
        var version = bytes[offset++];
        if (version != _schema.Version)
            throw new InvalidOperationException(
                $"Schema version mismatch: expected {_schema.Version}, got {version}");

        // Create instance
        var instance = Activator.CreateInstance<T>();

        // Read each field
        foreach (var field in _schema.Fields)
        {
            var value = ReadField(bytes, ref offset, field);
            SetFieldValue(instance, field.Name, value);
        }

        return instance;
    }

    public int GetSize(object obj)
    {
        return CalculateSize();
    }

    public bool CanSerialize(Type type)
    {
        return _schema.Matches(type);
    }

    private int CalculateSize()
    {
        var size = 1; // Schema version byte

        foreach (var field in _schema.Fields) size += field.GetSize();

        return size;
    }

    private void WriteField(byte[] bytes, ref int offset, Field field, object? value)
    {
        switch (field.Type)
        {
            case FieldType.Int:
                var intVal = value != null ? (int)value : 0;
                BitConverter.GetBytes(intVal).CopyTo(bytes, offset);
                offset += sizeof(int);
                break;

            case FieldType.Long:
                var longVal = value != null ? (long)value : 0L;
                BitConverter.GetBytes(longVal).CopyTo(bytes, offset);
                offset += sizeof(long);
                break;

            case FieldType.String:
                var strVal = value?.ToString() ?? "";
                var strBytes = Encoding.UTF8.GetBytes(strVal.PadRight(field.MaxLength ?? 0));
                Array.Copy(strBytes, 0, bytes, offset, field.MaxLength ?? 0);
                offset += field.MaxLength ?? 0;
                break;

            case FieldType.DateTime:
                var dtVal = value != null ? (DateTime)value : DateTime.MinValue;
                BitConverter.GetBytes(dtVal.Ticks).CopyTo(bytes, offset);
                offset += sizeof(long);
                break;

            case FieldType.Bool:
                var boolVal = value != null && (bool)value;
                bytes[offset++] = boolVal ? (byte)1 : (byte)0;
                break;

            case FieldType.Double:
                var doubleVal = value != null ? (double)value : 0.0;
                BitConverter.GetBytes(doubleVal).CopyTo(bytes, offset);
                offset += sizeof(double);
                break;

            default:
                throw new NotSupportedException($"Field type {field.Type} not supported");
        }
    }

    private object? ReadField(byte[] bytes, ref int offset, Field field)
    {
        object? value = null;

        switch (field.Type)
        {
            case FieldType.Int:
                value = BitConverter.ToInt32(bytes, offset);
                offset += sizeof(int);
                break;

            case FieldType.Long:
                value = BitConverter.ToInt64(bytes, offset);
                offset += sizeof(long);
                break;

            case FieldType.String:
                value = Encoding.UTF8.GetString(bytes, offset, field.MaxLength ?? 0).TrimEnd('\0');
                offset += field.MaxLength ?? 0;
                break;

            case FieldType.DateTime:
                var ticks = BitConverter.ToInt64(bytes, offset);
                value = new DateTime(ticks);
                offset += sizeof(long);
                break;

            case FieldType.Bool:
                value = bytes[offset++] != 0;
                break;

            case FieldType.Double:
                value = BitConverter.ToDouble(bytes, offset);
                offset += sizeof(double);
                break;
        }

        return value;
    }

    private object? GetFieldValue(object obj, string fieldName)
    {
        var property = obj.GetType().GetProperty(fieldName);
        return property?.GetValue(obj);
    }

    private void SetFieldValue(object obj, string fieldName, object? value)
    {
        var property = obj.GetType().GetProperty(fieldName);
        property?.SetValue(obj, value);
    }
}