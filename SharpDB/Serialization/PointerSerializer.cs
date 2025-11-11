using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Serialization;

public class PointerSerializer : ISerializer<Pointer>
{
    public int Size => Pointer.ByteSize;

    public byte[] Serialize(Pointer obj)
    {
        return obj.ToBytes();
    }

    public Pointer Deserialize(byte[] bytes, int offset = 0)
    {
        return Pointer.FromBytes(bytes, offset);
    }
}