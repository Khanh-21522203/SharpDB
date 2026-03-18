using System.Buffers.Binary;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class LongSerializer : ISerializer<long>
{
    public int Size => sizeof(long);

    public byte[] Serialize(long obj) => BitConverter.GetBytes(obj);

    public void SerializeTo(long obj, Span<byte> dest) =>
        BinaryPrimitives.WriteInt64LittleEndian(dest, obj);

    public long Deserialize(byte[] bytes, int offset = 0) =>
        BitConverter.ToInt64(bytes, offset);
}