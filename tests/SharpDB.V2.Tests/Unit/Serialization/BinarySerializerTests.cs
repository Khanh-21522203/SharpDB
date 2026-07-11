namespace SharpDB.V2.Tests.Unit.Serialization;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using SharpDB.V2.Engine.Serialization;
using Xunit;

public sealed class BinarySerializerTests
{
    public sealed class AllTypesRow
    {
        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public double DoubleValue { get; set; }
        public bool BoolValue { get; set; }
        public decimal DecimalValue { get; set; }
        public Guid GuidValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public DateTimeOffset DateTimeOffsetValue { get; set; }
        public string StringValue { get; set; } = "";
        public byte[] ByteArrayValue { get; set; } = [];
    }

    private sealed class NullableRow
    {
        public int? IntValue { get; set; }
        public long? LongValue { get; set; }
        public double? DoubleValue { get; set; }
        public bool? BoolValue { get; set; }
        public decimal? DecimalValue { get; set; }
        public Guid? GuidValue { get; set; }
        public DateTime? DateTimeValue { get; set; }
        public DateTimeOffset? DateTimeOffsetValue { get; set; }
        public string? StringValue { get; set; }
        public byte[]? ByteArrayValue { get; set; }
    }

    private sealed class UnsupportedRow
    {
        public List<int> Items { get; set; } = new();
    }

    private sealed class NoParameterlessCtorRow
    {
        public NoParameterlessCtorRow(int x) => X = x;
        public int X { get; set; }
    }

    private sealed class SingleStringRow
    {
        public string? Text { get; set; }
    }

    private sealed class ThreeIntRow
    {
        public int First { get; set; }
        public int Second { get; set; }
        public int Third { get; set; }
    }

    private sealed class NineNullableIntRow
    {
        public int? F0 { get; set; }
        public int? F1 { get; set; }
        public int? F2 { get; set; }
        public int? F3 { get; set; }
        public int? F4 { get; set; }
        public int? F5 { get; set; }
        public int? F6 { get; set; }
        public int? F7 { get; set; }
        public int? F8 { get; set; }
    }

    private sealed class FiveNullableIntRow
    {
        public int? F0 { get; set; }
        public int? F1 { get; set; }
        public int? F2 { get; set; }
        public int? F3 { get; set; }
        public int? F4 { get; set; }
    }

    private sealed class CountingStringRow
    {
        public int TextGetCount;
        private string? _text;

        public string? Text
        {
            get { TextGetCount++; return _text; }
            set => _text = value;
        }
    }

    [Fact]
    public void RoundTrips_AllPrimitiveTypes()
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow
        {
            IntValue = -42,
            LongValue = 123456789012345L,
            DoubleValue = 3.14159,
            BoolValue = true,
            DecimalValue = 12345.6789m,
            GuidValue = Guid.NewGuid(),
            DateTimeValue = new DateTime(2026, 7, 11, 10, 30, 0, DateTimeKind.Utc),
            DateTimeOffsetValue = new DateTimeOffset(2026, 7, 11, 10, 30, 0, TimeSpan.FromHours(7)),
            StringValue = "hello world",
            ByteArrayValue = [1, 2, 3, 4, 5]
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.IntValue.Should().Be(value.IntValue);
        result.LongValue.Should().Be(value.LongValue);
        result.DoubleValue.Should().Be(value.DoubleValue);
        result.BoolValue.Should().Be(value.BoolValue);
        result.DecimalValue.Should().Be(value.DecimalValue);
        result.GuidValue.Should().Be(value.GuidValue);
        result.DateTimeValue.Should().Be(value.DateTimeValue);
        result.DateTimeOffsetValue.Should().Be(value.DateTimeOffsetValue);
        result.DateTimeOffsetValue.Offset.Should().Be(value.DateTimeOffsetValue.Offset);
        result.StringValue.Should().Be(value.StringValue);
        result.ByteArrayValue.Should().Equal(value.ByteArrayValue);
    }

    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Local)]
    [InlineData(DateTimeKind.Unspecified)]
    public void RoundTrips_DateTimeKind(DateTimeKind kind)
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { DateTimeValue = new DateTime(2026, 1, 1, 0, 0, 0, kind), StringValue = "", ByteArrayValue = [] };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.DateTimeValue.Should().Be(value.DateTimeValue);
        result.DateTimeValue.Kind.Should().Be(kind);
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("héllo wörld 你好 😀🎉")]
    public void RoundTrips_Strings(string text)
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { StringValue = text, ByteArrayValue = [] };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.StringValue.Should().Be(text);
    }

    [Fact]
    public void RoundTrips_LongString()
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var text = new string('x', 10_000);
        var value = new AllTypesRow { StringValue = text, ByteArrayValue = [] };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.StringValue.Should().Be(text);
    }

    public static IEnumerable<object[]> ByteArrayCases()
    {
        yield return [Array.Empty<byte>()];
        yield return [new byte[] { 7 }];
        yield return [Enumerable.Range(0, 5000).Select(i => (byte)i).ToArray()];
    }

    [Theory]
    [MemberData(nameof(ByteArrayCases))]
    public void RoundTrips_ByteArrays(byte[] bytes)
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { StringValue = "", ByteArrayValue = bytes };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.ByteArrayValue.Should().Equal(bytes);
    }

    [Fact]
    public void Nullable_PresentValues_RoundTrip()
    {
        var serializer = new BinarySerializer<NullableRow>();
        var value = new NullableRow
        {
            IntValue = 7,
            LongValue = 8L,
            DoubleValue = 1.5,
            BoolValue = true,
            DecimalValue = 9.99m,
            GuidValue = Guid.NewGuid(),
            DateTimeValue = new DateTime(2026, 1, 1),
            DateTimeOffsetValue = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            StringValue = "present",
            ByteArrayValue = [9, 9]
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.Should().BeEquivalentTo(value);
    }

    [Fact]
    public void Nullable_AllAbsent_RoundTrip()
    {
        var serializer = new BinarySerializer<NullableRow>();
        var value = new NullableRow();

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.IntValue.Should().BeNull();
        result.LongValue.Should().BeNull();
        result.DoubleValue.Should().BeNull();
        result.BoolValue.Should().BeNull();
        result.DecimalValue.Should().BeNull();
        result.GuidValue.Should().BeNull();
        result.DateTimeValue.Should().BeNull();
        result.DateTimeOffsetValue.Should().BeNull();
        result.StringValue.Should().BeNull();
        result.ByteArrayValue.Should().BeNull();
    }

    [Fact]
    public void Nullable_MixedPresenceAcrossManyFields_DoesNotCorruptSiblingFields()
    {
        var serializer = new BinarySerializer<NullableRow>();
        var value = new NullableRow
        {
            IntValue = 1,
            LongValue = null,
            DoubleValue = 2.5,
            BoolValue = null,
            DecimalValue = 3.5m,
            GuidValue = null,
            DateTimeValue = new DateTime(2026, 5, 5),
            DateTimeOffsetValue = null,
            StringValue = null,
            ByteArrayValue = [1, 2, 3]
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.IntValue.Should().Be(1);
        result.LongValue.Should().BeNull();
        result.DoubleValue.Should().Be(2.5);
        result.BoolValue.Should().BeNull();
        result.DecimalValue.Should().Be(3.5m);
        result.GuidValue.Should().BeNull();
        result.DateTimeValue.Should().Be(new DateTime(2026, 5, 5));
        result.DateTimeOffsetValue.Should().BeNull();
        result.StringValue.Should().BeNull();
        result.ByteArrayValue.Should().Equal(1, 2, 3);
    }

    public static IEnumerable<object[]> EstimateSizeCases()
    {
        yield return [new AllTypesRow { StringValue = "", ByteArrayValue = [] }];
        yield return [new AllTypesRow { StringValue = "short", ByteArrayValue = [1, 2, 3] }];
        yield return [new AllTypesRow { StringValue = new string('a', 500), ByteArrayValue = new byte[1000] }];
    }

    [Theory]
    [MemberData(nameof(EstimateSizeCases))]
    public void EstimateSize_MatchesActualBytesWritten(AllTypesRow value)
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var estimated = serializer.EstimateSize(value);

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        writer.WrittenCount.Should().Be(estimated);
    }

    [Fact]
    public void Deserialize_Throws_OnUnrecognizedFormatVersion()
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { StringValue = "", ByteArrayValue = [] };
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        var corrupted = writer.WrittenSpan.ToArray();
        corrupted[0] = 99;

        var act = () => serializer.Deserialize(corrupted);
        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Constructor_Throws_ForUnsupportedPropertyType()
    {
        var act = () => new BinarySerializer<UnsupportedRow>();
        act.Should().Throw<NotSupportedException>().WithMessage("*Items*");
    }

    [Fact]
    public void Constructor_Throws_ForMissingParameterlessConstructor()
    {
        var act = () => new BinarySerializer<NoParameterlessCtorRow>();
        act.Should().Throw<NotSupportedException>().WithMessage("*parameterless constructor*");
    }

    [Fact]
    public void RoundTrips_RuntimeNullString_OnNonNullableAnnotatedProperty()
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { StringValue = null!, ByteArrayValue = [] };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.StringValue.Should().BeNull();
    }

    [Fact]
    public void RoundTrips_RuntimeNullByteArray_OnNonNullableAnnotatedProperty()
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { StringValue = "", ByteArrayValue = null! };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.ByteArrayValue.Should().BeNull();
    }

    [Fact]
    public void Deserialize_Throws_OnTruncatedInput()
    {
        var serializer = new BinarySerializer<AllTypesRow>();
        var value = new AllTypesRow { StringValue = "hello", ByteArrayValue = [1, 2, 3] };
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        var truncated = writer.WrittenSpan[..^5].ToArray();

        var act = () => serializer.Deserialize(truncated);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Deserialize_Throws_OnCorruptedLengthPrefix()
    {
        var serializer = new BinarySerializer<SingleStringRow>();
        var value = new SingleStringRow { Text = "hello" };
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        var corrupted = writer.WrittenSpan.ToArray();
        // Layout: [0]=version [1]=presence [2..6)=length prefix [6..)=payload.
        BinaryPrimitives.WriteInt32LittleEndian(corrupted.AsSpan(2, 4), int.MaxValue);

        var act = () => serializer.Deserialize(corrupted);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void FieldOrder_IsDeterministic_ByMetadataToken()
    {
        var serializer = new BinarySerializer<ThreeIntRow>();
        var value = new ThreeIntRow { First = 111, Second = 222, Third = 333 };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var bytes = writer.WrittenSpan;

        // Wire format (v2+): 1 version byte, then per field, in declaration/MetadataToken order, just the
        // 4-byte int payload — non-nullable value fields carry no presence byte. This test pins that
        // contract; it must be updated deliberately (not silently) if a later phase changes the layout again.
        writer.WrittenCount.Should().Be(13);
        BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(1, 4)).Should().Be(111);
        BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(5, 4)).Should().Be(222);
        BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(9, 4)).Should().Be(333);
    }

    [Fact]
    public void Deserialize_Throws_OnInvalidPresenceValue()
    {
        var serializer = new BinarySerializer<SingleStringRow>();
        var value = new SingleStringRow { Text = "hello" };
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        var corrupted = writer.WrittenSpan.ToArray();
        // Layout: [0]=version [1]=presence [2..6)=length prefix [6..)=payload.
        corrupted[1] = 2;

        var act = () => serializer.Deserialize(corrupted);
        act.Should().Throw<NotSupportedException>();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void RoundTrips_MultiByteMask_AcrossByteBoundary(bool f7Present, bool f8Present)
    {
        // 9 presence-bearing fields need a 2-byte mask: F7 is the last bit of mask byte 0,
        // F8 is the first bit of mask byte 1 — this pins that the bit index correctly crosses the boundary.
        var serializer = new BinarySerializer<NineNullableIntRow>();
        var value = new NineNullableIntRow
        {
            F0 = 0, F1 = 1, F2 = 2, F3 = 3, F4 = 4, F5 = 5, F6 = 6,
            F7 = f7Present ? 77 : null,
            F8 = f8Present ? 88 : null
        };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);
        var result = serializer.Deserialize(writer.WrittenSpan);

        result.F0.Should().Be(0);
        result.F1.Should().Be(1);
        result.F2.Should().Be(2);
        result.F3.Should().Be(3);
        result.F4.Should().Be(4);
        result.F5.Should().Be(5);
        result.F6.Should().Be(6);
        result.F7.Should().Be(f7Present ? 77 : null);
        result.F8.Should().Be(f8Present ? 88 : null);
    }

    [Fact]
    public void Serialize_UsesTwoMaskBytes_ForNinePresenceBearingFields()
    {
        var serializer = new BinarySerializer<NineNullableIntRow>();
        var value = new NineNullableIntRow { F0 = 0, F1 = 1, F2 = 2, F3 = 3, F4 = 4, F5 = 5, F6 = 6, F7 = 7, F8 = 8 };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        // 1 version byte + 2 mask bytes (ceil(9/8) = 2) + 9 x 4-byte int payloads.
        writer.WrittenCount.Should().Be(1 + 2 + 9 * 4);
    }

    [Fact]
    public void Deserialize_Throws_OnReservedMaskBitSet()
    {
        // 5 presence-bearing fields use a 1-byte mask with 3 reserved (always-zero) high bits.
        var serializer = new BinarySerializer<FiveNullableIntRow>();
        var value = new FiveNullableIntRow { F0 = 1, F1 = null, F2 = 2, F3 = null, F4 = 3 };
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        var corrupted = writer.WrittenSpan.ToArray();
        // Layout: [0]=version [1]=mask byte (bits 0-4 defined, bits 5-7 reserved). Set a reserved bit.
        corrupted[1] |= 0b0010_0000;

        var act = () => serializer.Deserialize(corrupted);
        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void Deserialize_Throws_OnTruncatedMask()
    {
        var serializer = new BinarySerializer<NineNullableIntRow>();
        var value = new NineNullableIntRow { F0 = 0, F1 = 1, F2 = 2, F3 = 3, F4 = 4, F5 = 5, F6 = 6, F7 = 7, F8 = 8 };
        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        // Truncate to the version byte + first mask byte only — the second mask byte is missing entirely.
        var truncated = writer.WrittenSpan[..2].ToArray();

        var act = () => serializer.Deserialize(truncated);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void EstimateSize_IsConstant_ForFixedOnlySchema()
    {
        // ThreeIntRow has no presence-bearing fields, so it uses the fixed-only fast path — Measure
        // must return the exact same constant regardless of field values, not just the same formula.
        var serializer = new BinarySerializer<ThreeIntRow>();

        var a = serializer.EstimateSize(new ThreeIntRow { First = 0, Second = 0, Third = 0 });
        var b = serializer.EstimateSize(new ThreeIntRow { First = int.MinValue, Second = int.MaxValue, Third = -1 });
        var c = serializer.EstimateSize(new ThreeIntRow { First = 111, Second = 222, Third = 333 });

        a.Should().Be(13);
        b.Should().Be(13);
        c.Should().Be(13);
    }

    [Fact]
    public void Serialize_ReadsStringPropertyOnlyOnce()
    {
        // Before Phase 5, Serialize read a string field's property getter up to 5 times across two
        // independent Measure/Write passes (2 in Measure: null-check + MeasureString; 3 in Write:
        // null-check + MeasureString + WriteString), with the O(n) UTF-8 byte count computed twice.
        // The merged SerializeInto path caches the value once and reuses it for everything.
        var serializer = new BinarySerializer<CountingStringRow>();
        var value = new CountingStringRow { Text = "hello world" };

        var writer = new ArrayBufferWriter<byte>();
        serializer.Serialize(value, writer);

        value.TextGetCount.Should().Be(1);
    }
}
