namespace SharpDB.V2.Benchmarks;

/// <summary>Lets the generic <see cref="SerializationBenchmarks{T}"/> build a representative instance
/// of each row shape without reflection, via a static-abstract factory method.</summary>
public interface IBenchmarkRow<TSelf> where TSelf : IBenchmarkRow<TSelf>
{
    static abstract TSelf CreateSample();
}

/// <summary>All fixed-width, non-nullable value fields — no presence-byte payload variability, no strings/byte[].</summary>
public sealed class FixedOnlyRow : IBenchmarkRow<FixedOnlyRow>
{
    public int IntValue { get; set; }
    public long LongValue { get; set; }
    public double DoubleValue { get; set; }
    public bool BoolValue { get; set; }
    public decimal DecimalValue { get; set; }
    public Guid GuidValue { get; set; }
    public DateTime DateTimeValue { get; set; }
    public DateTimeOffset DateTimeOffsetValue { get; set; }

    public static FixedOnlyRow CreateSample() => new()
    {
        IntValue = 42,
        LongValue = 1_234_567_890_123L,
        DoubleValue = 3.14159,
        BoolValue = true,
        DecimalValue = 9876.5432m,
        GuidValue = Guid.Parse("11111111-2222-3333-4444-555555555555"),
        DateTimeValue = new DateTime(2026, 7, 11, 10, 0, 0, DateTimeKind.Utc),
        DateTimeOffsetValue = new DateTimeOffset(2026, 7, 11, 10, 0, 0, TimeSpan.FromHours(4))
    };
}

/// <summary>Same fields as <see cref="FixedOnlyRow"/> but nullable, with an even present/absent mix
/// so the presence-branch cost in the compiled Write/Read delegates is representative.</summary>
public sealed class NullableHeavyRow : IBenchmarkRow<NullableHeavyRow>
{
    public int? IntValue { get; set; }
    public long? LongValue { get; set; }
    public double? DoubleValue { get; set; }
    public bool? BoolValue { get; set; }
    public decimal? DecimalValue { get; set; }
    public Guid? GuidValue { get; set; }
    public DateTime? DateTimeValue { get; set; }
    public DateTimeOffset? DateTimeOffsetValue { get; set; }

    public static NullableHeavyRow CreateSample() => new()
    {
        IntValue = 42,
        LongValue = null,
        DoubleValue = 3.14159,
        BoolValue = null,
        DecimalValue = 9876.5432m,
        GuidValue = null,
        DateTimeValue = new DateTime(2026, 7, 11, 10, 0, 0, DateTimeKind.Utc),
        DateTimeOffsetValue = null
    };
}

/// <summary>Several string fields spanning short ASCII, medium ASCII, and multi-byte UTF-8 content.</summary>
public sealed class StringHeavyRow : IBenchmarkRow<StringHeavyRow>
{
    public string ShortAscii { get; set; } = "";
    public string MediumAscii { get; set; } = "";
    public string Unicode { get; set; } = "";
    public string LongAscii { get; set; } = "";
    public string EmptyValue { get; set; } = "";
    public string? NullValue { get; set; }

    public static StringHeavyRow CreateSample() => new()
    {
        ShortAscii = "hello",
        MediumAscii = new string('a', 200),
        Unicode = "héllo wörld 你好 😀🎉 привет",
        LongAscii = new string('x', 5_000),
        EmptyValue = "",
        NullValue = null
    };
}

/// <summary>Several byte[] fields spanning empty, small, ~1KB, and ~16KB payloads.</summary>
public sealed class ByteArrayHeavyRow : IBenchmarkRow<ByteArrayHeavyRow>
{
    public byte[] Empty { get; set; } = [];
    public byte[] Small { get; set; } = [];
    public byte[] Medium { get; set; } = [];
    public byte[] Large { get; set; } = [];
    public byte[]? NullValue { get; set; }

    public static ByteArrayHeavyRow CreateSample() => new()
    {
        Empty = [],
        Small = [1, 2, 3, 4, 5, 6, 7, 8],
        Medium = CreateBytes(1_024),
        Large = CreateBytes(16_384),
        NullValue = null
    };

    private static byte[] CreateBytes(int length)
    {
        var bytes = new byte[length];
        for (var i = 0; i < length; i++)
            bytes[i] = (byte)i;
        return bytes;
    }
}
