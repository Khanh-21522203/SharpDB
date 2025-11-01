using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Operations;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;

namespace SharpDB.Operations;

public class CollectionInsertOperation<T, TKey>(
    IDataIOSession dataSession,
    IObjectSerializer serializer,
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    int collectionId,
    T record,
    Func<T, TKey> keyExtractor)
    : ICollectionOperation
    where T : class
    where TKey : IComparable<TKey>
{
    public async Task ExecuteAsync()
    {
        // Extract primary key
        var primaryKey = keyExtractor(record);

        // Check if key already exists
        var existing = await primaryIndex.GetAsync(primaryKey);
        if (existing != null)
            throw new InvalidOperationException($"Record with key {primaryKey} already exists");

        // Serialize record
        var data = serializer.Serialize(record);

        // Store data (returns pointer to storage location)
        var pointer = await dataSession.StoreAsync(
            1,
            collectionId,
            1,
            data);

        // Update primary index: key → pointer
        await primaryIndex.PutAsync(primaryKey, pointer);
    }
}