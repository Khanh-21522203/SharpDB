using System.Runtime.InteropServices;

namespace SharpDB.V2.Engine.Primitives;

// Many-to-one lock-free variable-length ring buffer.
// Cache-line padded head/tail to prevent false sharing.
[StructLayout(LayoutKind.Sequential)]
public sealed class LockFreeRingBuffer : IDisposable
{
    public LockFreeRingBuffer(int capacity) { }

    public bool TryWrite(ReadOnlySpan<byte> data) => false;

    public bool TryRead(Span<byte> buffer, out int bytesRead) { bytesRead = 0; return false; }

    public void Dispose() { }
}
