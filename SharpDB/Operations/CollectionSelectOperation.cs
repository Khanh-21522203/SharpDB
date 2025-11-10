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
        var pointerNullable = await primaryIndex.GetAsync(primaryKey);
        // For Pointer? (nullable struct), check if it's null or has invalid position
        if (pointerNullable == null || pointerNullable is { Position: <= 0 })
        {
            Result = null;
            return;
        }
        
        // Get the actual pointer value
        var pointer = (DataStructures.Pointer)pointerNullable;

        // Debug: Check what type of pointer we got from the index
        if (pointer.Type != DataStructures.Pointer.TypeData)
        {
            // The pointer from index is wrong type - this should not happen
            throw new InvalidOperationException($"Expected TypeData pointer from index but got type {pointer.Type}");
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