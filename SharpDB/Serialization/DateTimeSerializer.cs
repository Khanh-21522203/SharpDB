using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class DateTimeSerializer : ISerializer<DateTime>
{
    public int Size => sizeof(long);
    
    public byte[] Serialize(DateTime obj)
    {
        var ticks = obj.Ticks;
        return BitConverter.GetBytes(ticks);
    }
    
    public DateTime Deserialize(byte[] bytes, int offset = 0)
    {
        var ticks = BitConverter.ToInt64(bytes, offset);
        return new DateTime(ticks);
    }
}
