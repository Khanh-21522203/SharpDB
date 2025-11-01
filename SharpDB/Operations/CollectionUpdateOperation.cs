using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Operations;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;

namespace SharpDB.Operations;

public class CollectionUpdateOperation<T, TKey>(
    IDataIOSession dataSession,
    IObjectSerializer serializer,
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    T record,
    Func<T, TKey> keyExtractor)
    : ICollectionOperation
    where T : class
    where TKey : IComparable<TKey>
{
    public async Task ExecuteAsync()
    {
        // Extract primary key from record
        var primaryKey = keyExtractor(record);
        
        // Find existing record pointer
        var pointer = await primaryIndex.GetAsync(primaryKey);
        if (pointer == null)
            throw new InvalidOperationException($"Record with key {primaryKey} not found");
        
        // Serialize updated data
        var data = serializer.Serialize(record);
        
        // Update storage at existing pointer location
        await dataSession.UpdateAsync(pointer, data);
    }
}