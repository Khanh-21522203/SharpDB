namespace SharpDB.V2.Benchmarks;

using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using SharpDB.V2.Engine.Serialization;

/// <summary>A fixed-capacity <see cref="IBufferWriter{T}"/> reused across benchmark invocations via
/// <see cref="Reset"/>, so a benchmark's reported allocations reflect <see cref="BinarySerializer{T}"/>
/// itself rather than the writer's own buffer growth.</summary>
internal sealed class FixedBufferWriter(int capacity) : IBufferWriter<byte>
{
    private readonly byte[] _buffer = new byte[capacity];
    private int _position;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);

    public void Reset() => _position = 0;

    public void Advance(int count) => _position += count;

    public Memory<byte> GetMemory(int sizeHint = 0) => _buffer.AsMemory(_position);

    public Span<byte> GetSpan(int sizeHint = 0) => _buffer.AsSpan(_position);
}

/// <summary>Steady-state Serialize/Deserialize/Measure/RoundTrip cost for <see cref="BinarySerializer{T}"/>,
/// run against each representative row shape. The serializer instance is built once in <see cref="Setup"/>
/// (outside the measured methods) so these benchmarks isolate the compiled delegates' per-call cost, not
/// <see cref="CompiledSerializerFactory"/>'s one-time reflection/compile cost — see
/// <see cref="SerializationColdStartBenchmarks"/> for that.</summary>
[MemoryDiagnoser]
[GenericTypeArguments(typeof(FixedOnlyRow))]
[GenericTypeArguments(typeof(NullableHeavyRow))]
[GenericTypeArguments(typeof(StringHeavyRow))]
[GenericTypeArguments(typeof(ByteArrayHeavyRow))]
public class SerializationBenchmarks<T> where T : class, IBenchmarkRow<T>, new()
{
    private BinarySerializer<T> _serializer = null!;
    private T _value = null!;
    private byte[] _serializedBytes = null!;
    private FixedBufferWriter _writer = null!;

    [GlobalSetup]
    public void Setup()
    {
        _serializer = new BinarySerializer<T>();
        _value = T.CreateSample();

        var probe = new ArrayBufferWriter<byte>();
        _serializer.Serialize(_value, probe);
        _serializedBytes = probe.WrittenSpan.ToArray();

        _writer = new FixedBufferWriter(_serializedBytes.Length * 2 + 256);
    }

    [Benchmark]
    public int Measure() => _serializer.EstimateSize(_value);

    [Benchmark]
    public void Serialize()
    {
        _writer.Reset();
        _serializer.Serialize(_value, _writer);
    }

    [Benchmark]
    public T Deserialize() => _serializer.Deserialize(_serializedBytes);

    [Benchmark]
    public T RoundTrip()
    {
        _writer.Reset();
        _serializer.Serialize(_value, _writer);
        return _serializer.Deserialize(_writer.WrittenSpan);
    }
}

/// <summary>Isolates the one-time cost of <c>CompiledSerializerFactory.GetOrBuild&lt;T&gt;()</c> — reflection
/// scan + <c>Expression.Compile()</c> — for a <typeparamref name="T"/> never seen before in the process.
/// <see cref="RunStrategy.ColdStart"/> launches a fresh process per measurement, which is what actually
/// resets the internal per-Type cache (no InternalsVisibleTo exists to reach into it directly).</summary>
[SimpleJob(RunStrategy.ColdStart, launchCount: 5, warmupCount: 0, iterationCount: 1)]
public class SerializationColdStartBenchmarks
{
    [Benchmark]
    public BinarySerializer<FixedOnlyRow> FixedOnly() => new();

    [Benchmark]
    public BinarySerializer<NullableHeavyRow> NullableHeavy() => new();

    [Benchmark]
    public BinarySerializer<StringHeavyRow> StringHeavy() => new();

    [Benchmark]
    public BinarySerializer<ByteArrayHeavyRow> ByteArrayHeavy() => new();
}

/// <summary>Reference lower bound for <see cref="FixedOnlyRow"/>: hand-written, non-reflective,
/// non-Expression-tree code (<see cref="ManualFixedOnlyRowCodec"/>) doing the same encoding directly.
/// Compare against <see cref="SerializationBenchmarks{T}"/> instantiated with <see cref="FixedOnlyRow"/>
/// to see how close the compiled fixed-only fast path gets to hand-tuned code.</summary>
[MemoryDiagnoser]
public class ManualCodecReferenceBenchmarks
{
    private FixedOnlyRow _value = null!;
    private byte[] _serializedBytes = null!;
    private byte[] _destination = null!;

    [GlobalSetup]
    public void Setup()
    {
        _value = FixedOnlyRow.CreateSample();
        _destination = new byte[ManualFixedOnlyRowCodec.Size];
        ManualFixedOnlyRowCodec.Serialize(_value, _destination);
        _serializedBytes = (byte[])_destination.Clone();
    }

    [Benchmark]
    public int Measure() => ManualFixedOnlyRowCodec.Measure(_value);

    [Benchmark]
    public void Serialize() => ManualFixedOnlyRowCodec.Serialize(_value, _destination);

    [Benchmark]
    public FixedOnlyRow Deserialize() => ManualFixedOnlyRowCodec.Deserialize(_serializedBytes);
}
