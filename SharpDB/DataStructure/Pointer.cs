namespace SharpDB.DataStructure;

public enum PointerType : byte
{
    TypeData = 0x01,
    TypeNode = 0x02
}

public class Pointer(PointerType type, long position, int chunk) : IComparable<Pointer>
{
    private const int TypeSize = sizeof(byte);
    private const int LongSize = sizeof(long);
    private const int IntSize = sizeof(int);
    public static readonly int Bytes = TypeSize + LongSize + IntSize;
    
    private PointerType _type = type;
    private long _position = position;
    private int _chunk = chunk;

    public static Pointer Empty => new Pointer(0, 0, 0);
    public static Pointer FromBytes(byte[] bytes, int position)
    {
        if (position < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(position));
        }

        var type = (PointerType)bytes[position];
        var pos = BitConverter.ToInt64(bytes, position + TypeSize);
        var chunk = BitConverter.ToInt32(bytes, position + TypeSize + LongSize);

        return new Pointer(type, pos, chunk);
    }
    
    public static Pointer FromBytes(byte[] bytes) => FromBytes(bytes, 0);
    
    public byte[] ToBytes()
    {
        var bytes = new byte[Bytes];
        bytes[0] = (byte)_type;

        var posBytes = BitConverter.GetBytes(_position);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(posBytes);
        }
        Array.Copy(posBytes, 0, bytes, TypeSize, LongSize);

        var chunkBytes = BitConverter.GetBytes(_chunk);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(chunkBytes);
        }
        Array.Copy(chunkBytes, 0, bytes, TypeSize + LongSize, IntSize);

        return bytes;
    }
    
    public int CompareTo(Pointer? other)
    {
        if (other == null) return 1;
        if (_type != other._type)
        {
            return _type.CompareTo(other._type);
        }
        if (_position != other._position)
        {
            return _position.CompareTo(other._position);
        }
        return _chunk.CompareTo(other._chunk);
    }
}