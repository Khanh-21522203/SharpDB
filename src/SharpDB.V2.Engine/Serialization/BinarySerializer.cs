using System.Buffers;
using SharpDB.V2.Engine.Abstractions.Serialization;
using SharpDB.V2.Engine.Schema;

namespace SharpDB.V2.Engine.Serialization;

/// <summary>
/// General-purpose <see cref="ISerializer{T}"/> for POCO/record row types: reflects <typeparamref name="T"/>'s
/// public read-write properties once (via <see cref="CompiledSerializerFactory"/>) and encodes them with a
/// leading format-version byte, a shared presence bitmask for <see cref="Nullable{T}"/>/<see cref="string"/>/
/// <see cref="byte"/>[] fields (non-nullable fields carry no presence bit at all), then each field's payload.
/// Supported property types: <see cref="int"/>, <see cref="long"/>, <see cref="double"/>, <see cref="bool"/>,
/// <see cref="decimal"/>, <see cref="Guid"/>, <see cref="DateTime"/>, <see cref="DateTimeOffset"/>,
/// <see cref="string"/>, <see cref="byte"/>[], and <see cref="Nullable{T}"/> of each value type. Anything
/// else throws <see cref="NotSupportedException"/> at construction time.
/// </summary>
public sealed class BinarySerializer<T> : ISerializer<T>
{
    private readonly TypePlan<T> _plan;

    public BinarySerializer()
    {
        _plan = CompiledSerializerFactory.GetOrBuild<T>();
    }

    public BinarySerializer(CollectionSchema schema)
    {
        _plan = CompiledSerializerFactory.GetOrBuild<T>(schema);
    }

    public void Serialize(T value, IBufferWriter<byte> writer) => _plan.SerializeInto(value, writer);

    public T Deserialize(scoped in ReadOnlySpan<byte> data) => _plan.Read(data);

    public int EstimateSize(T value) => _plan.Measure(value);
}
