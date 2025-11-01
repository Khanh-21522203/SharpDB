using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Core.Abstractions.Sessions;

public interface IDataIOSession : IDisposable
{
    Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data);
    Task<DBObject?> SelectAsync(Pointer pointer);
    Task UpdateAsync(Pointer pointer, byte[] data);
    Task DeleteAsync(Pointer pointer);
    IAsyncEnumerable<DBObject> ScanAsync(int collectionId);
    Task FlushAsync();
}