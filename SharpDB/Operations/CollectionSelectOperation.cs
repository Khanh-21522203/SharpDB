using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Operations;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;

namespace SharpDB.Operations;

public class CollectionSelectOperation<T, TKey>(
    IDataIOSession dataSession,
    IObjectSerializer serializer,
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    TKey primaryKey)
    : ICollectionOperation
    where T : class
    where TKey : IComparable<TKey>
{
    public T? Result { get; private set; }

    public async Task ExecuteAsync()
    {
        // Look up pointer in primary index
        var pointer = await primaryIndex.GetAsync(primaryKey);
        if (pointer == null)
        {
            Result = null;
            return;
        }

        // Read data from storage using pointer
        var dbObject = await dataSession.SelectAsync(pointer);
        if (dbObject == null)
        {
            Result = null;
            return;
        }

        // Deserialize data to object
        Result = serializer.Deserialize<T>(dbObject.Data);
    }
}