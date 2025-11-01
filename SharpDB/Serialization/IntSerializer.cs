using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class IntSerializer : ISerializer<int>
{
    public int Size => sizeof(int);

    public byte[] Serialize(int obj)
    {
        return BitConverter.GetBytes(obj);
    }

    public int Deserialize(byte[] bytes, int offset = 0)
    {
        return BitConverter.ToInt32(bytes, offset);
    }
}