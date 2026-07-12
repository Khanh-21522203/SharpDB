namespace SharpDB.V2.Engine.Primitives;

public sealed class PinnedBufferPool : IDisposable
{
    public PinnedBufferPool(int bufferSize, int capacity) { }

    public PinnedBuffer Rent() => throw new NotImplementedException();

    public void Return(PinnedBuffer buffer) { }

    public void Dispose() { }
}
