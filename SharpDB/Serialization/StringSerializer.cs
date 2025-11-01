using System.Text;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

public class StringSerializer(int maxLength) : ISerializer<string>
{
    public int Size => maxLength;

    public byte[] Serialize(string obj)
    {
        var bytes = new byte[maxLength];
        var encoded = Encoding.UTF8.GetBytes(obj);
        Array.Copy(encoded, bytes, Math.Min(encoded.Length, maxLength));
        return bytes;
    }

    public string Deserialize(byte[] bytes, int offset = 0)
    {
        return Encoding.UTF8.GetString(bytes, offset, maxLength).TrimEnd('\0');
    }
}