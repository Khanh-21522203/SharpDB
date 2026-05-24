namespace SharpDB.V2.Engine.Abstractions.Storage;

public interface IPageStore : IDisposable
{
    IPageBuffer OpenBuffer(int capacity);
    void Sync();
    long PageCount { get; }
}
