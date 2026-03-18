using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

/// <summary>
/// Serializer for System.Guid type. Guid is 16 bytes (128 bits).
/// </summary>
public class GuidSerializer : ISerializer<Guid>
{
    private const int GuidSize = 16;

    public int Size => GuidSize;

    public byte[] Serialize(Guid value) => value.ToByteArray();

    public void SerializeTo(Guid value, Span<byte> dest) => value.TryWriteBytes(dest);

    public Guid Deserialize(byte[] buffer, int offset = 0) =>
        new Guid(new ReadOnlySpan<byte>(buffer, offset, GuidSize));
}
