using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;

namespace SharpDB.Engine.Writes;

public sealed class CollectionWriteOrchestrator<TRecord, TKey>(
    IDataIOSession dataSession,
    IObjectSerializer serializer,
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    int collectionId,
    Func<TRecord, TKey> keyExtractor,
    Func<TRecord, Pointer, CancellationToken, ValueTask>? onSecondaryInsert = null,
    Func<TRecord, CancellationToken, ValueTask>? onSecondaryDelete = null)
    : ICollectionWriteOrchestrator<TRecord, TKey>
    where TRecord : class
    where TKey : IComparable<TKey>
{
    public async ValueTask InsertAsync(TRecord record, CancellationToken ct = default)
    {
        var primaryKey = keyExtractor(record);
        var existingPointer = await primaryIndex.GetAsync(primaryKey);

        if (IsValidDataPointer(existingPointer))
            throw new InvalidOperationException($"Record with key {primaryKey} already exists");

        var data = serializer.Serialize(record);
        var pointer = await dataSession.StoreAsync(1, collectionId, 1, data);
        await primaryIndex.PutAsync(primaryKey, pointer);

        if (onSecondaryInsert != null)
            await onSecondaryInsert(record, pointer, ct);
    }

    public async ValueTask<bool> UpdateAsync(TRecord record, bool upsert = false, CancellationToken ct = default)
    {
        var primaryKey = keyExtractor(record);
        var pointer = await primaryIndex.GetAsync(primaryKey);

        if (!IsValidDataPointer(pointer))
        {
            if (!upsert)
                throw new InvalidOperationException($"Record with key {primaryKey} not found");

            await InsertAsync(record, ct);
            return true;
        }

        TRecord? oldRecord = null;
        if (onSecondaryDelete != null || onSecondaryInsert != null)
        {
            var current = await dataSession.SelectAsync(pointer);
            if (current != null)
                oldRecord = serializer.Deserialize<TRecord>(current.Data);
        }

        var data = serializer.Serialize(record);
        await dataSession.UpdateAsync(pointer, data);

        if (oldRecord != null && onSecondaryDelete != null)
            await onSecondaryDelete(oldRecord, ct);

        if (onSecondaryInsert != null)
            await onSecondaryInsert(record, pointer, ct);

        return true;
    }

    public async ValueTask<bool> DeleteAsync(TKey key, CancellationToken ct = default)
    {
        var pointer = await primaryIndex.GetAsync(key);
        if (!IsValidDataPointer(pointer))
            return false;

        TRecord? recordToDelete = null;
        if (onSecondaryDelete != null)
        {
            var current = await dataSession.SelectAsync(pointer);
            if (current != null)
                recordToDelete = serializer.Deserialize<TRecord>(current.Data);
        }

        await dataSession.DeleteAsync(pointer);
        await primaryIndex.RemoveAsync(key);

        if (recordToDelete != null && onSecondaryDelete != null)
            await onSecondaryDelete(recordToDelete, ct);

        return true;
    }

    private static bool IsValidDataPointer(Pointer pointer)
    {
        return pointer.Type == Pointer.TypeData && pointer.Position > 0;
    }
}
