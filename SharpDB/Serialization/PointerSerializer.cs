using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Serialization;

public class PointerSerializer : ISerializer<Pointer>
{
    public int Size => Pointer.ByteSize;

    public byte[] Serialize(Pointer obj) => obj.ToBytes();

    public void SerializeTo(Pointer obj, Span<byte> dest) => obj.FillBytes(dest);

    public Pointer Deserialize(byte[] bytes, int offset = 0) =>
        Pointer.FromBytes(bytes, offset);
}