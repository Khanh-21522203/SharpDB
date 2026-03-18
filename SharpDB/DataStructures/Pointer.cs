using System.Buffers.Binary;

namespace SharpDB.DataStructures;

public record struct Pointer(byte Type, long Position, int Chunk) : IComparable<Pointer>
{
    public const byte TypeData = 0x01;
    public const byte TypeNode = 0x02;
    public const int ByteSize = 1 + sizeof(long) + sizeof(int); // 13 bytes

    public byte Type { get; init; } = Type;
    public long Position { get; init; } = Position;
    public int Chunk { get; init; } = Chunk;

    public int CompareTo(Pointer other)
    {
        var chunkComp = Chunk.CompareTo(other.Chunk);
        if (chunkComp != 0) return chunkComp;

        return Position.CompareTo(other.Position);
    }

    public bool IsDataPointer()
    {
        return Type == TypeData;
    }

    public bool IsNodePointer()
    {
        return Type == TypeNode;
    }

    public static Pointer Empty()
    {
        return new Pointer(0x00, 0, 0);
    }

    public bool IsEmpty()
    {
        return Type == 0x00;
    }

    public byte[] ToBytes()
    {
        var buffer = new byte[ByteSize];
        FillBytes(buffer, 0);
        return buffer;
    }

    public void FillBytes(byte[] target, int offset)
    {
        target[offset] = Type;
        BinaryPrimitives.WriteInt64LittleEndian(target.AsSpan(offset + 1, 8), Position);
        BinaryPrimitives.WriteInt32LittleEndian(target.AsSpan(offset + 9, 4), Chunk);
    }

    public void FillBytes(Span<byte> dest)
    {
        dest[0] = Type;
        BinaryPrimitives.WriteInt64LittleEndian(dest[1..], Position);
        BinaryPrimitives.WriteInt32LittleEndian(dest[9..], Chunk);
    }

    public static Pointer FromBytes(byte[] bytes, int offset = 0)
    {
        var type = bytes[offset];
        var position = BitConverter.ToInt64(bytes, offset + 1);
        var chunk = BitConverter.ToInt32(bytes, offset + 9);
        return new Pointer(type, position, chunk);
    }

    public override string ToString()
    {
        var typeStr = Type switch
        {
            TypeData => "Data",
            TypeNode => "Node",
            _ => "Unknown"
        };
        return $"Pointer({typeStr}, pos={Position}, chunk={Chunk})";
    }
}