using SharpDB.V2.Engine.Abstractions.Storage;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Storage;

public sealed class Page : IPage
{
    public PageId Id => throw new NotImplementedException();
    public ReadOnlySpan<byte> Data => throw new NotImplementedException();
    public bool IsDirty => throw new NotImplementedException();

    public Span<byte> GetWritableData() => throw new NotImplementedException();

    public void MarkDirty() { }
}
