using SharpDB.V2.Engine.Abstractions.Storage;

namespace SharpDB.V2.Engine.Storage;

public sealed class PageStore : IPageStore
{
    public long PageCount => throw new NotImplementedException();

    public IPageBuffer OpenBuffer(int capacity) => throw new NotImplementedException();

    public void Sync() { }

    public void Dispose() { }
}
