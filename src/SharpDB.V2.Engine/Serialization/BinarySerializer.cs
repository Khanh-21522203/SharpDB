using System.Buffers;
using SharpDB.V2.Engine.Abstractions.Serialization;

namespace SharpDB.V2.Engine.Serialization;

public sealed class BinarySerializer<T> : ISerializer<T>
{
    public void Serialize(T value, IBufferWriter<byte> writer) { }

    public T Deserialize(scoped in ReadOnlySpan<byte> data) => throw new NotImplementedException();

    public int EstimateSize(T value) => throw new NotImplementedException();
}
