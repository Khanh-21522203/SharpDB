using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Serialization;

/// <summary>
///     Central registry mapping types to their serializers and key bounds.
///     Allows the B+Tree module to be used with any type without internal changes.
///     Call Register&lt;T&gt; before using a custom type as TK or TV.
/// </summary>
public static class SerializerRegistry
{
    private record Entry(object Serializer, object Min, object Max);

    private static readonly Dictionary<Type, Entry> _entries = new();

    static SerializerRegistry()
    {
        Register(new LongSerializer(), long.MinValue, long.MaxValue);
        Register(new IntSerializer(), int.MinValue, int.MaxValue);
        Register(new StringSerializer(255), string.Empty, new string('\uffff', 255));
        Register(new DateTimeSerializer(), DateTime.MinValue, DateTime.MaxValue);
        Register(new DecimalSerializer(), decimal.MinValue, decimal.MaxValue);
        Register(new GuidSerializer(), Guid.Empty, new Guid(new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }));
        Register(new PointerSerializer(), Pointer.Empty(), Pointer.Empty());
    }

    /// <summary>
    ///     Register a type with its serializer and key bounds (required for range queries).
    /// </summary>
    public static void Register<T>(ISerializer<T> serializer, T min, T max)
    {
        _entries[typeof(T)] = new Entry(serializer, min!, max!);
    }

    /// <summary>
    ///     Get the serializer for type T. Throws if type is not registered.
    /// </summary>
    public static ISerializer<T> GetSerializer<T>()
    {
        if (!_entries.TryGetValue(typeof(T), out var entry))
            throw new NotSupportedException(
                $"Type '{typeof(T).Name}' is not registered in SerializerRegistry. " +
                $"Call SerializerRegistry.Register<{typeof(T).Name}>(...) before use.");

        return (ISerializer<T>)entry.Serializer;
    }

    /// <summary>
    ///     Get the minimum value for type T (used in range/GreaterThan queries).
    /// </summary>
    public static T GetMinValue<T>()
    {
        if (!_entries.TryGetValue(typeof(T), out var entry))
            throw new NotSupportedException(
                $"Type '{typeof(T).Name}' is not registered in SerializerRegistry.");

        return (T)entry.Min;
    }

    /// <summary>
    ///     Get the maximum value for type T (used in range/LessThan queries).
    /// </summary>
    public static T GetMaxValue<T>()
    {
        if (!_entries.TryGetValue(typeof(T), out var entry))
            throw new NotSupportedException(
                $"Type '{typeof(T).Name}' is not registered in SerializerRegistry.");

        return (T)entry.Max;
    }
}
