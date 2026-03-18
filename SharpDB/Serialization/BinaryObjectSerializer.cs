using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
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
    // Cache PropertyInfo per (Type, fieldName) to avoid repeated reflection lookups.
    private static readonly ConcurrentDictionary<(Type, string), PropertyInfo?> PropertyCache = new();

    // Compiled deserializer cache: (Type, schemaVersion, fieldCount) → Func<byte[], int, T>
    // The compiled lambda reads fields directly at compile-time offsets — no boxing, no reflection per call.
    private static readonly ConcurrentDictionary<(Type, int, int), Delegate> DeserializerCache = new();

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

    public T Deserialize<T>(byte[] bytes) where T : class =>
        Deserialize<T>(bytes, 0);

    /// <summary>
    ///     Zero-copy overload: reads directly from <paramref name="bytes"/> at <paramref name="offset"/>.
    ///     Uses a compiled expression-tree lambda — no reflection per call, no boxing of value types.
    /// </summary>
    public T Deserialize<T>(byte[] bytes, int offset) where T : class
    {
        if (bytes == null || bytes.Length == 0)
            throw new ArgumentException("Bytes cannot be null or empty", nameof(bytes));

        var version = bytes[offset];
        if (version != _schema.Version)
            throw new InvalidOperationException(
                $"Schema version mismatch: expected {_schema.Version}, got {version}");

        return GetOrBuildDeserializer<T>()(bytes, offset);
    }

    public int GetSize(object obj) => CalculateSize();

    public bool CanSerialize(Type type) => _schema.Matches(type);

    // ── private helpers ──────────────────────────────────────────────────────

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
                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset, 4), intVal);
                offset += sizeof(int);
                break;

            case FieldType.Long:
                var longVal = value != null ? (long)value : 0L;
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(offset, 8), longVal);
                offset += sizeof(long);
                break;

            case FieldType.String:
                var strVal = value?.ToString() ?? "";
                var maxLen = field.MaxLength ?? 0;
                var written = Encoding.UTF8.GetBytes(strVal, 0, Math.Min(strVal.Length, maxLen), bytes, offset);
                // Zero-pad remainder (buffer is already zero-init'd on first use, but may be reused).
                if (written < maxLen)
                    bytes.AsSpan(offset + written, maxLen - written).Clear();
                offset += maxLen;
                break;

            case FieldType.DateTime:
                var dtVal = value != null ? (DateTime)value : DateTime.MinValue;
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(offset, 8), dtVal.Ticks);
                offset += sizeof(long);
                break;

            case FieldType.Bool:
                var boolVal = value != null && (bool)value;
                bytes[offset++] = boolVal ? (byte)1 : (byte)0;
                break;

            case FieldType.Double:
                var doubleVal = value != null ? (double)value : 0.0;
                BinaryPrimitives.WriteDoubleLittleEndian(bytes.AsSpan(offset, 8), doubleVal);
                offset += sizeof(double);
                break;

            default:
                throw new NotSupportedException($"Field type {field.Type} not supported");
        }
    }

    private static object? GetFieldValue(object obj, string fieldName)
    {
        var property = PropertyCache.GetOrAdd((obj.GetType(), fieldName), key => key.Item1.GetProperty(key.Item2));
        return property?.GetValue(obj);
    }

    // ── compiled deserializer ─────────────────────────────────────────────────

    private Func<byte[], int, T> GetOrBuildDeserializer<T>() where T : class
    {
        var key = (typeof(T), _schema.Version, _schema.Fields.Count);
        if (DeserializerCache.TryGetValue(key, out var cached))
            return (Func<byte[], int, T>)cached;

        var compiled = BuildCompiledDeserializer<T>();
        DeserializerCache.TryAdd(key, compiled);
        return compiled;
    }

    /// <summary>
    ///     Builds a compiled lambda <c>Func&lt;byte[], int, T&gt;</c> that:
    ///     <list type="bullet">
    ///       <item>Creates an instance via direct <c>new T()</c> (no Activator reflection).</item>
    ///       <item>Reads each field at a compile-time-constant relative offset from startOffset.</item>
    ///       <item>Sets properties directly — no boxing of value types.</item>
    ///     </list>
    ///     Compiled once per (T, schema) pair and cached for the lifetime of the process.
    /// </summary>
    private Func<byte[], int, T> BuildCompiledDeserializer<T>() where T : class
    {
        var type = typeof(T);
        var ctor = type.GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException(
                       $"Type {type.Name} requires a public parameterless constructor for deserialization");

        var bytesParam = Expression.Parameter(typeof(byte[]), "bytes");
        var startParam = Expression.Parameter(typeof(int), "startOffset");
        var instance = Expression.Variable(type, "obj");
        var body = new List<Expression> { Expression.Assign(instance, Expression.New(ctor)) };

        // Skip schema version byte (byte 0 at startOffset).
        var relativeOffset = 1;

        foreach (var field in _schema.Fields)
        {
            var prop = type.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            // Absolute index expression: startOffset + relativeOffset (constant folded by JIT).
            var absOffset = Expression.Add(startParam, Expression.Constant(relativeOffset));

            Expression fieldExpr;
            switch (field.Type)
            {
                case FieldType.Int:
                    fieldExpr = Expression.Call(
                        typeof(BitConverter).GetMethod(nameof(BitConverter.ToInt32),
                            new[] { typeof(byte[]), typeof(int) })!,
                        bytesParam, absOffset);
                    relativeOffset += sizeof(int);
                    break;

                case FieldType.Long:
                    fieldExpr = Expression.Call(
                        typeof(BitConverter).GetMethod(nameof(BitConverter.ToInt64),
                            new[] { typeof(byte[]), typeof(int) })!,
                        bytesParam, absOffset);
                    relativeOffset += sizeof(long);
                    break;

                case FieldType.String:
                    var maxLen = field.MaxLength ?? 0;
                    fieldExpr = Expression.Call(
                        typeof(BinaryObjectSerializer).GetMethod(nameof(ReadStringField),
                            BindingFlags.Static | BindingFlags.NonPublic)!,
                        bytesParam, absOffset, Expression.Constant(maxLen));
                    relativeOffset += maxLen;
                    break;

                case FieldType.DateTime:
                    fieldExpr = Expression.Call(
                        typeof(BinaryObjectSerializer).GetMethod(nameof(ReadDateTimeField),
                            BindingFlags.Static | BindingFlags.NonPublic)!,
                        bytesParam, absOffset);
                    relativeOffset += sizeof(long);
                    break;

                case FieldType.Bool:
                    fieldExpr = Expression.NotEqual(
                        Expression.ArrayIndex(bytesParam, absOffset),
                        Expression.Constant((byte)0));
                    relativeOffset += 1;
                    break;

                case FieldType.Double:
                    fieldExpr = Expression.Call(
                        typeof(BitConverter).GetMethod(nameof(BitConverter.ToDouble),
                            new[] { typeof(byte[]), typeof(int) })!,
                        bytesParam, absOffset);
                    relativeOffset += sizeof(double);
                    break;

                default:
                    relativeOffset += field.GetSize();
                    continue;
            }

            if (prop?.CanWrite == true)
                body.Add(Expression.Assign(Expression.Property(instance, prop), fieldExpr));
        }

        body.Add(instance);
        var block = Expression.Block(type, new[] { instance }, body);
        return Expression.Lambda<Func<byte[], int, T>>(block, bytesParam, startParam).Compile();
    }

    private static string ReadStringField(byte[] bytes, int offset, int maxLen)
    {
        var span = new ReadOnlySpan<byte>(bytes, offset, maxLen);
        var lastNonNull = span.LastIndexOfAnyExcept((byte)0);
        return lastNonNull < 0 ? string.Empty : Encoding.UTF8.GetString(span[..(lastNonNull + 1)]);
    }

    private static DateTime ReadDateTimeField(byte[] bytes, int offset) =>
        new DateTime(BitConverter.ToInt64(bytes, offset));
}
