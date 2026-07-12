using System.Buffers;

namespace SharpDB.V2.Engine.Abstractions.Serialization;

public interface ISerializer<T>
{
    void Serialize(T value, IBufferWriter<byte> writer);
    T Deserialize(scoped in ReadOnlySpan<byte> data);
    int EstimateSize(T value);
}
