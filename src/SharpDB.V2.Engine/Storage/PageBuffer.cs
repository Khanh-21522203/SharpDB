using SharpDB.V2.Engine.Abstractions.Storage;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Storage;

public sealed class PageBuffer : IPageBuffer
{
    public IPage Get(PageId id) => throw new NotImplementedException();

    public IPage Allocate() => throw new NotImplementedException();

    public void Pin(PageId id) { }

    public void Unpin(PageId id) { }

    public void FlushDirty() { }

    public void Dispose() { }
}
