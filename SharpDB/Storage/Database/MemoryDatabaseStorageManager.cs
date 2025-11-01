using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Storage.Database;

/// <summary>
///     In-memory database storage for testing.
/// </summary>
public class MemoryDatabaseStorageManager : IDatabaseStorageManager
{
    private readonly
        ConcurrentDictionary<string, (int SchemeId, int CollectionId, int Version, byte[] Data, bool IsDeleted)>
        _records = new();

    private long _nextPosition;

    public Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data)
    {
        var position = Interlocked.Increment(ref _nextPosition);
        var pointer = new Pointer(Pointer.TypeData, position, 0);
        var key = pointer.ToString();

        _records[key] = (schemeId, collectionId, version, data, false);

        return Task.FromResult(pointer);
    }

    public Task<DBObject?> SelectAsync(Pointer pointer)
    {
        var key = pointer.ToString();

        if (!_records.TryGetValue(key, out var record))
            return Task.FromResult<DBObject?>(null);

        if (record.IsDeleted)
            return Task.FromResult<DBObject?>(null);

        // Create a mock DBObject
        var page = new Page.Page(0, 4096);
        var dbObj = page.AllocateObject(record.SchemeId, record.CollectionId, record.Version, record.Data);

        return Task.FromResult(dbObj);
    }

    public Task UpdateAsync(Pointer pointer, byte[] data)
    {
        var key = pointer.ToString();

        if (_records.TryGetValue(key, out var record))
            _records[key] = (record.SchemeId, record.CollectionId, record.Version, data, record.IsDeleted);

        return Task.CompletedTask;
    }

    public Task DeleteAsync(Pointer pointer)
    {
        var key = pointer.ToString();

        if (_records.TryGetValue(key, out var record))
            _records[key] = (record.SchemeId, record.CollectionId, record.Version, record.Data, true);

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<DBObject> ScanAsync(int collectionId)
    {
        var page = new Page.Page(0, 4096);

        foreach (var kvp in _records)
        {
            var record = kvp.Value;

            if (record.CollectionId == collectionId && !record.IsDeleted)
            {
                var dbObj = page.AllocateObject(record.SchemeId, record.CollectionId, record.Version, record.Data);
                if (dbObj != null)
                    yield return dbObj;
            }
        }

        await Task.CompletedTask;
    }

    public Task FlushAsync()
    {
        // No-op for memory storage
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _records.Clear();
    }
}