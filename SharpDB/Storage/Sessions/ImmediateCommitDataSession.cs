using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Storage.Sessions;

public class ImmediateCommitDataSession(IDatabaseStorageManager storage) : IDataIOSession
{
    public Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data)
    {
        return storage.StoreAsync(schemeId, collectionId, version, data);
    }
    
    public Task<DBObject?> SelectAsync(Pointer pointer)
    {
        return storage.SelectAsync(pointer);
    }
    
    public Task UpdateAsync(Pointer pointer, byte[] data)
    {
        return storage.UpdateAsync(pointer, data);
    }
    
    public Task DeleteAsync(Pointer pointer)
    {
        return storage.DeleteAsync(pointer);
    }
    
    public IAsyncEnumerable<DBObject> ScanAsync(int collectionId)
    {
        return storage.ScanAsync(collectionId);
    }
    
    public Task FlushAsync() => Task.CompletedTask;
    
    public void Dispose() { }
}
