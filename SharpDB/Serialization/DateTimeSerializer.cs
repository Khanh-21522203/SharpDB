using System.Buffers.Binary;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class DateTimeSerializer : ISerializer<DateTime>
{
    public int Size => sizeof(long);

    public byte[] Serialize(DateTime obj) => BitConverter.GetBytes(obj.Ticks);

    public void SerializeTo(DateTime obj, Span<byte> dest) =>
        BinaryPrimitives.WriteInt64LittleEndian(dest, obj.Ticks);

    public DateTime Deserialize(byte[] bytes, int offset = 0) =>
        new DateTime(BitConverter.ToInt64(bytes, offset));
}