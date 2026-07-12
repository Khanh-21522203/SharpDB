namespace SharpDB.V2.Engine.Primitives;

public sealed unsafe class PinnedBuffer : IDisposable
{
    public int Length { get; }

    public Span<byte> Span => new(Pointer, Length);

    public ReadOnlySpan<byte> ReadOnlySpan => new(Pointer, Length);

    public byte* Pointer { get; }

    public void Dispose() { }
}
