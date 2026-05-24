namespace SharpDB.V2.Tests.Helpers;
using SharpDB.V2.Engine.Abstractions.Storage;

public sealed class FakePageStore : IPageStore
{
    public IPageBuffer OpenBuffer(int capacity) => throw new NotImplementedException();
    public void Sync() { }
    public long PageCount => throw new NotImplementedException();
    public void Dispose() { }
}
