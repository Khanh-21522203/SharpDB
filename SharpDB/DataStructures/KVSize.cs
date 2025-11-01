namespace SharpDB.DataStructures;

/// <summary>
/// Stores key and value sizes for an index.
/// Immutable value type.
/// </summary>
public record KvSize(int KeySize, int ValueSize)
{
    public const int ByteSize = 8; // 4 + 4
    
    public byte[] ToBytes()
    {
        var bytes = new byte[ByteSize];
        BitConverter.GetBytes(KeySize).CopyTo(bytes, 0);
        BitConverter.GetBytes(ValueSize).CopyTo(bytes, 4);
        return bytes;
    }
    
    public static KvSize FromBytes(byte[] bytes, int offset = 0)
    {
        var keySize = BitConverter.ToInt32(bytes, offset);
        var valueSize = BitConverter.ToInt32(bytes, offset + 4);
        return new KvSize(keySize, valueSize);
    }
    
    public override string ToString() => $"KVSize(K={KeySize}, V={ValueSize})";
}