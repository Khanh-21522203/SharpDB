using SharpDB.Configuration;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Storage.Sessions;

public class BufferedDataIOSession : IDataIOSession
{
    private readonly IPageDataWorkspace _workspace;

    public BufferedDataIOSession(IPageDataWorkspace workspace)
    {
        _workspace = workspace;
    }

    public BufferedDataIOSession(IDatabaseStorageManager storage, EngineConfig config)
        : this(new PageDataWorkspace(storage, config.PageSize))
    {
    }

    public async Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data)
    {
        return await _workspace.ApplyAsync(new DataMutation.Insert(
            collectionId,
            schemeId,
            version,
            data,
            Durability.Buffered));
    }

    public async Task<DBObject?> SelectAsync(Pointer pointer)
    {
        return await _workspace.ReadAsync(pointer, ReadVisibility.Session);
    }

    public async Task UpdateAsync(Pointer pointer, byte[] data)
    {
        await _workspace.ApplyAsync(new DataMutation.Update(pointer, data, Durability.Buffered));
    }

    public async Task DeleteAsync(Pointer pointer)
    {
        await _workspace.ApplyAsync(new DataMutation.Delete(pointer, Durability.Buffered));
    }

    public IAsyncEnumerable<DBObject> ScanAsync(int collectionId)
    {
        return _workspace.ScanAsync(collectionId, ScanVisibility.Committed);
    }

    public async Task FlushAsync()
    {
        await _workspace.ApplyAsync(new DataMutation.Commit());
    }

    public void Dispose()
    {
        FlushAsync().Wait();
    }
}
