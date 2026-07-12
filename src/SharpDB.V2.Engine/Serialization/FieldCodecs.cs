using System.Buffers.Binary;
using System.Text;

namespace SharpDB.V2.Engine.Serialization;

/// <summary>
/// Plain, non-reflective read/write primitives for the fixed set of property types
/// <see cref="BinarySerializer{T}"/> supports. Kept as ordinary static methods (rather than
/// inlined into the compiled expression trees in <see cref="CompiledSerializerFactory"/>) so the
/// byte-level encoding logic is easy to read, debug, and unit test directly; the compiled
/// delegates only ever call these methods, they never manipulate span elements themselves.
/// </summary>
internal static class FieldCodecs
{
    public const int Int32Size = sizeof(int);
    public const int Int64Size = sizeof(long);
    public const int DoubleSize = sizeof(double);
    public const int BooleanSize = 1;
    public const int DecimalSize = 16;
    public const int GuidSize = 16;
    public const int DateTimeSize = sizeof(long);
    public const int DateTimeOffsetSize = sizeof(long) + sizeof(short);
    public const int LengthPrefixSize = sizeof(int);
    public const byte FormatVersion = 3;

    public static void WriteByte(Span<byte> dest, byte value) => dest[0] = value;
    public static byte ReadByte(ReadOnlySpan<byte> src) => src[0];

    public static void ValidateVersion(byte actual)
    {
        if (actual != FormatVersion)
            throw new NotSupportedException(
                $"BinarySerializer format version mismatch: expected {FormatVersion}, got {actual}.");
    }

    /// <summary>Validates a presence-mask byte: only the low <paramref name="usedBitCount"/> bits may be
    /// set (the remaining, unused bits of the last mask byte must be zero) — catches corrupted input
    /// instead of silently misreading a field's presence.</summary>
    public static void ValidateMask(byte value, int usedBitCount)
    {
        var usedMask = usedBitCount >= 8 ? (byte)0xFF : (byte)((1 << usedBitCount) - 1);
        if ((value & ~usedMask) != 0)
            throw new NotSupportedException(
                $"BinarySerializer encountered reserved bits set in a presence mask byte (value {value}, only {usedBitCount} bit(s) defined).");
    }

    public static void WriteInt32(Span<byte> dest, int value) => BinaryPrimitives.WriteInt32LittleEndian(dest, value);
    public static int ReadInt32(ReadOnlySpan<byte> src) => BinaryPrimitives.ReadInt32LittleEndian(src);

    public static void WriteInt64(Span<byte> dest, long value) => BinaryPrimitives.WriteInt64LittleEndian(dest, value);
    public static long ReadInt64(ReadOnlySpan<byte> src) => BinaryPrimitives.ReadInt64LittleEndian(src);

    public static void WriteDouble(Span<byte> dest, double value) => BinaryPrimitives.WriteDoubleLittleEndian(dest, value);
    public static double ReadDouble(ReadOnlySpan<byte> src) => BinaryPrimitives.ReadDoubleLittleEndian(src);

    public static void WriteBoolean(Span<byte> dest, bool value) => dest[0] = value ? (byte)1 : (byte)0;
    public static bool ReadBoolean(ReadOnlySpan<byte> src) => src[0] != 0;

    public static void WriteDecimal(Span<byte> dest, decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        for (var i = 0; i < 4; i++)
            BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(i * 4, 4), bits[i]);
    }

    public static decimal ReadDecimal(ReadOnlySpan<byte> src)
    {
        Span<int> bits = stackalloc int[4];
        for (var i = 0; i < 4; i++)
            bits[i] = BinaryPrimitives.ReadInt32LittleEndian(src.Slice(i * 4, 4));
        return new decimal(bits);
    }

    public static void WriteGuid(Span<byte> dest, Guid value) => value.TryWriteBytes(dest);
    public static Guid ReadGuid(ReadOnlySpan<byte> src) => new(src);

    public static void WriteDateTime(Span<byte> dest, DateTime value) =>
        BinaryPrimitives.WriteInt64LittleEndian(dest, value.ToBinary());

    public static DateTime ReadDateTime(ReadOnlySpan<byte> src) =>
        DateTime.FromBinary(BinaryPrimitives.ReadInt64LittleEndian(src));

    public static void WriteDateTimeOffset(Span<byte> dest, DateTimeOffset value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(0, 8), value.Ticks);
        BinaryPrimitives.WriteInt16LittleEndian(dest.Slice(8, 2), (short)value.Offset.TotalMinutes);
    }

    public static DateTimeOffset ReadDateTimeOffset(ReadOnlySpan<byte> src)
    {
        var ticks = BinaryPrimitives.ReadInt64LittleEndian(src.Slice(0, 8));
        var offsetMinutes = BinaryPrimitives.ReadInt16LittleEndian(src.Slice(8, 2));
        return new DateTimeOffset(ticks, TimeSpan.FromMinutes(offsetMinutes));
    }

    public static int MeasureString(string value) => Encoding.UTF8.GetByteCount(value);
    public static void WriteString(Span<byte> dest, string value) => Encoding.UTF8.GetBytes(value, dest);
    public static string ReadString(ReadOnlySpan<byte> src) => Encoding.UTF8.GetString(src);

    public static void WriteBytes(Span<byte> dest, byte[] value) => value.CopyTo(dest);
    public static byte[] ReadBytes(ReadOnlySpan<byte> src) => src.ToArray();
}
