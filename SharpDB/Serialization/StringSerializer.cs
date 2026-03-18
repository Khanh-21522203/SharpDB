using System.Text;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class StringSerializer(int maxLength) : ISerializer<string>
{
    public int Size => maxLength;

    public byte[] Serialize(string obj)
    {
        var bytes = new byte[maxLength];
        SerializeTo(obj, bytes);
        return bytes;
    }

    public void SerializeTo(string obj, Span<byte> dest)
    {
        // Encode directly into dest (no intermediate byte[]), null-pad the remainder.
        var written = Encoding.UTF8.GetBytes(obj.AsSpan(), dest);
        dest[written..].Clear();
    }

    public string Deserialize(byte[] bytes, int offset = 0)
    {
        var span = new ReadOnlySpan<byte>(bytes, offset, maxLength);
        // SIMD scan for last non-null byte to avoid TrimEnd('\0') creating a second string.
        var lastNonNull = span.LastIndexOfAnyExcept((byte)0);
        var actualLength = lastNonNull < 0 ? 0 : lastNonNull + 1;
        return Encoding.UTF8.GetString(span[..actualLength]);
    }
}