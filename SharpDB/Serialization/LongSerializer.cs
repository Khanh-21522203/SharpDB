using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class LongSerializer : ISerializer<long>
{
    public int Size => sizeof(long);

    public byte[] Serialize(long obj)
    {
        return BitConverter.GetBytes(obj);
    }

    public long Deserialize(byte[] bytes, int offset = 0)
    {
        return BitConverter.ToInt64(bytes, offset);
    }
}