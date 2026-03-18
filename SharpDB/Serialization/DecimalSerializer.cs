using System.Runtime.InteropServices;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class DecimalSerializer : ISerializer<decimal>
{
    public int Size => sizeof(decimal);

    public byte[] Serialize(decimal obj)
    {
        var bytes = new byte[16];
        SerializeTo(obj, bytes);
        return bytes;
    }

    public void SerializeTo(decimal obj, Span<byte> dest)
    {
        Span<int> bits = stackalloc int[4];
        decimal.TryGetBits(obj, bits, out _);
        MemoryMarshal.Cast<int, byte>(bits).CopyTo(dest);
    }

    public decimal Deserialize(byte[] bytes, int offset = 0)
    {
        var bits = new int[4];
        Buffer.BlockCopy(bytes, offset, bits, 0, 16);
        return new decimal(bits);
    }
}