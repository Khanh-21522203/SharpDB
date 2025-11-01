using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Serialization;

/// <summary>
///     Serializes BinaryList to binary format.
///     Uses existing BinaryList.ToBytes() and FromBytes() methods.
/// </summary>
public class BinaryListSerializer<TV> : ISerializer<BinaryList<TV>>
    where TV : IComparable<TV>
{
    private readonly ISerializer<TV> _itemSerializer;

    public BinaryListSerializer(ISerializer<TV> itemSerializer)
    {
        _itemSerializer = itemSerializer;
        // Size = 4 bytes (count) + max items * item size
        Size = 4 + 100 * _itemSerializer.Size; // Max 100 items per key
    }

    public int Size { get; }

    public byte[] Serialize(BinaryList<TV> list)
    {
        var bytes = list.ToBytes();

        // Pad to fixed size
        if (bytes.Length < Size)
        {
            var padded = new byte[Size];
            Array.Copy(bytes, 0, padded, 0, bytes.Length);
            return padded;
        }

        return bytes;
    }

    public BinaryList<TV> Deserialize(byte[] bytes, int offset = 0)
    {
        return BinaryList<TV>.FromBytes(bytes, _itemSerializer, offset);
    }
}