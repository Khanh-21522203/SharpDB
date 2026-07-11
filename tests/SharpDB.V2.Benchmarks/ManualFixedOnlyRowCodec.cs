namespace SharpDB.V2.Benchmarks;

using System.Buffers.Binary;

/// <summary>Hand-written, non-reflective, non-Expression-tree reference codec for <see cref="FixedOnlyRow"/>'s
/// exact 8-field shape — a "how fast could this possibly be" lower bound to compare
/// <see cref="SharpDB.V2.Engine.Serialization.BinarySerializer{T}"/>'s compiled fast path against. Mirrors the
/// engine's wire format (1 version byte + each field's fixed-width payload, no presence bits) using
/// <see cref="BinaryPrimitives"/> directly, since the engine's internal codec helpers aren't visible here.</summary>
internal static class ManualFixedOnlyRowCodec
{
    private const byte FormatVersion = 3;

    public const int Size = 1 + 4 + 8 + 8 + 1 + 16 + 16 + 8 + 10;

    public static int Measure(FixedOnlyRow value) => Size;

    public static void Serialize(FixedOnlyRow value, Span<byte> dest)
    {
        var offset = 0;
        dest[offset] = FormatVersion;
        offset += 1;

        BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(offset, 4), value.IntValue);
        offset += 4;

        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(offset, 8), value.LongValue);
        offset += 8;

        BinaryPrimitives.WriteDoubleLittleEndian(dest.Slice(offset, 8), value.DoubleValue);
        offset += 8;

        dest[offset] = value.BoolValue ? (byte)1 : (byte)0;
        offset += 1;

        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value.DecimalValue, bits);
        for (var i = 0; i < 4; i++)
            BinaryPrimitives.WriteInt32LittleEndian(dest.Slice(offset + i * 4, 4), bits[i]);
        offset += 16;

        value.GuidValue.TryWriteBytes(dest.Slice(offset, 16));
        offset += 16;

        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(offset, 8), value.DateTimeValue.ToBinary());
        offset += 8;

        BinaryPrimitives.WriteInt64LittleEndian(dest.Slice(offset, 8), value.DateTimeOffsetValue.Ticks);
        BinaryPrimitives.WriteInt16LittleEndian(dest.Slice(offset + 8, 2), (short)value.DateTimeOffsetValue.Offset.TotalMinutes);
    }

    public static FixedOnlyRow Deserialize(ReadOnlySpan<byte> data)
    {
        var offset = 0;
        var version = data[offset];
        if (version != FormatVersion)
            throw new NotSupportedException($"Format version mismatch: expected {FormatVersion}, got {version}.");
        offset += 1;

        var intValue = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset, 4));
        offset += 4;

        var longValue = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset, 8));
        offset += 8;

        var doubleValue = BinaryPrimitives.ReadDoubleLittleEndian(data.Slice(offset, 8));
        offset += 8;

        var boolValue = data[offset] != 0;
        offset += 1;

        Span<int> bits = stackalloc int[4];
        for (var i = 0; i < 4; i++)
            bits[i] = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset + i * 4, 4));
        var decimalValue = new decimal(bits);
        offset += 16;

        var guidValue = new Guid(data.Slice(offset, 16));
        offset += 16;

        var dateTimeValue = DateTime.FromBinary(BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset, 8)));
        offset += 8;

        var ticks = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset, 8));
        var offsetMinutes = BinaryPrimitives.ReadInt16LittleEndian(data.Slice(offset + 8, 2));
        var dateTimeOffsetValue = new DateTimeOffset(ticks, TimeSpan.FromMinutes(offsetMinutes));

        return new FixedOnlyRow
        {
            IntValue = intValue,
            LongValue = longValue,
            DoubleValue = doubleValue,
            BoolValue = boolValue,
            DecimalValue = decimalValue,
            GuidValue = guidValue,
            DateTimeValue = dateTimeValue,
            DateTimeOffsetValue = dateTimeOffsetValue
        };
    }
}
