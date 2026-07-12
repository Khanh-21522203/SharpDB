namespace SharpDB.V2.Infrastructure.IO;
using SharpDB.V2.Engine.Abstractions.Storage;

public sealed class FilePageStore : IPageStore
{
    public FilePageStore(string path) { }
    public IPageBuffer OpenBuffer(int capacity) => throw new NotImplementedException();
    public void Sync() { }
    public long PageCount => throw new NotImplementedException();
    public void Dispose() { }
}
