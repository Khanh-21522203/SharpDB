using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Storage;

public interface IPageBuffer : IDisposable
{
    IPage Get(PageId id);
    IPage Allocate();
    void Pin(PageId id);
    void Unpin(PageId id);
    void FlushDirty();
}
