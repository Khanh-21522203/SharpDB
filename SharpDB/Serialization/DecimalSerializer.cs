using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class DecimalSerializer : ISerializer<decimal>
{
    public int Size => sizeof(decimal);
    
    public byte[] Serialize(decimal obj)
    {
        var bits = decimal.GetBits(obj);
        var bytes = new byte[16];
        
        Buffer.BlockCopy(bits, 0, bytes, 0, 16);
        return bytes;
    }
    
    public decimal Deserialize(byte[] bytes, int offset = 0)
    {
        var bits = new int[4];
        Buffer.BlockCopy(bytes, offset, bits, 0, 16);
        return new decimal(bits);
    }
}
