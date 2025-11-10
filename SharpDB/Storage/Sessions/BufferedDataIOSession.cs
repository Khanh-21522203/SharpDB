using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Storage.Sessions;

public class BufferedDataIOSession(IDatabaseStorageManager storage) : IDataIOSession
{
    private readonly HashSet<Pointer> _deleteBuffer = [];
    private readonly Dictionary<Pointer, byte[]> _updateBuffer = new();

    public async Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data)
    {
        // Directly store to get real pointer
        var pointer = await storage.StoreAsync(schemeId, collectionId, version, data);
        
        // Don't flush after every insert - let the caller decide when to flush
        
        return pointer;
    }

    public async Task<DBObject?> SelectAsync(Pointer pointer)
    {
        // Check if in update buffer
        if (_updateBuffer.TryGetValue(pointer, out var data))
        {
            // Return modified version
            var page = new Page.Page(0, 4096);
            return page.AllocateObject(0, 0, 1, data);
        }

        // Check if deleted
        if (_deleteBuffer.Contains(pointer))
            return null;

        return await storage.SelectAsync(pointer);
    }

    public Task UpdateAsync(Pointer pointer, byte[] data)
    {
        _updateBuffer[pointer] = data;
        return Task.CompletedTask;
    }

    public Task DeleteAsync(Pointer pointer)
    {
        _deleteBuffer.Add(pointer);
        return Task.CompletedTask;
    }

    public IAsyncEnumerable<DBObject> ScanAsync(int collectionId)
    {
        return storage.ScanAsync(collectionId);
    }

    public async Task FlushAsync()
    {
        // Process updates
        foreach (var (pointer, data) in _updateBuffer) await storage.UpdateAsync(pointer, data);
        _updateBuffer.Clear();

        // Process deletes
        foreach (var pointer in _deleteBuffer) await storage.DeleteAsync(pointer);
        _deleteBuffer.Clear();

        await storage.FlushAsync();
    }

    public void Dispose()
    {
        FlushAsync().Wait();
    }
}