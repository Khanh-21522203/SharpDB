using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Page;

public sealed class PageDataWorkspace(IDatabaseStorageManager storage, int pageSize) : IPageDataWorkspace
{
    private readonly HashSet<Pointer> _deleteBuffer = [];
    private readonly Dictionary<Pointer, byte[]> _updateBuffer = new();

    public async Task<Pointer> ApplyAsync(DataMutation mutation, CancellationToken ct = default)
    {
        switch (mutation)
        {
            case DataMutation.Insert insert:
                return await storage.StoreAsync(insert.SchemeId, insert.CollectionId, insert.Version, insert.Data);

            case DataMutation.Update update:
                if (update.Durability == Durability.Immediate)
                {
                    await storage.UpdateAsync(update.Pointer, update.Data);
                    return update.Pointer;
                }

                _updateBuffer[update.Pointer] = update.Data;
                _deleteBuffer.Remove(update.Pointer);
                return update.Pointer;

            case DataMutation.Delete delete:
                if (delete.Durability == Durability.Immediate)
                {
                    await storage.DeleteAsync(delete.Pointer);
                    return delete.Pointer;
                }

                _deleteBuffer.Add(delete.Pointer);
                _updateBuffer.Remove(delete.Pointer);
                return delete.Pointer;

            case DataMutation.Commit commit:
                await FlushBufferedChangesAsync(commit.CollectionId);
                return Pointer.Empty();

            default:
                throw new NotSupportedException($"Unsupported mutation: {mutation.GetType().Name}");
        }
    }

    public async Task<DBObject?> ReadAsync(Pointer pointer, ReadVisibility visibility = ReadVisibility.Session, CancellationToken ct = default)
    {
        if (visibility == ReadVisibility.Session)
        {
            if (_deleteBuffer.Contains(pointer))
                return null;

            if (_updateBuffer.TryGetValue(pointer, out var data))
            {
                var page = new Page(0, pageSize, pointer.Chunk);
                return page.AllocateObject(0, 1, data);
            }
        }

        return await storage.SelectAsync(pointer);
    }

    public IAsyncEnumerable<DBObject> ScanAsync(
        int collectionId,
        ScanVisibility visibility = ScanVisibility.Committed,
        CancellationToken ct = default)
    {
        // Buffered updates/deletes are pointer-addressed and not directly enumerable from scan.
        // Keep scan behavior committed-only to avoid inconsistent synthetic enumeration.
        return storage.ScanAsync(collectionId);
    }

    public async Task TruncateCollectionAsync(int collectionId)
    {
        // Clear pending buffered operations for this collection
        var deleteKeys = _deleteBuffer.Where(p => p.Chunk == collectionId).ToList();
        foreach (var ptr in deleteKeys) _deleteBuffer.Remove(ptr);
        var updateKeys = _updateBuffer.Keys.Where(p => p.Chunk == collectionId).ToList();
        foreach (var ptr in updateKeys) _updateBuffer.Remove(ptr);
        await storage.TruncateCollectionAsync(collectionId);
    }

    private async Task FlushBufferedChangesAsync(int collectionId)
    {
        if (_updateBuffer.Count > 0)
        {
            var updateKeys = new List<Pointer>(_updateBuffer.Count);
            foreach (var (pointer, data) in _updateBuffer)
            {
                if (collectionId >= 0 && pointer.Chunk != collectionId)
                    continue;

                await storage.UpdateAsync(pointer, data);
                updateKeys.Add(pointer);
            }
            foreach (var key in updateKeys)
                _updateBuffer.Remove(key);
        }

        if (_deleteBuffer.Count > 0)
        {
            var deleteKeys = new List<Pointer>(_deleteBuffer.Count);
            foreach (var pointer in _deleteBuffer)
            {
                if (collectionId >= 0 && pointer.Chunk != collectionId)
                    continue;

                await storage.DeleteAsync(pointer);
                deleteKeys.Add(pointer);
            }
            foreach (var key in deleteKeys)
                _deleteBuffer.Remove(key);
        }

        await storage.FlushAsync();
    }
}
