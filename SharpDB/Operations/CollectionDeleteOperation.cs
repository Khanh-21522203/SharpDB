using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Operations;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;

namespace SharpDB.Operations;

public class CollectionDeleteOperation<TKey>(
    IDataIOSession dataSession,
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    TKey primaryKey)
    : ICollectionOperation
    where TKey : IComparable<TKey>
{
    public bool WasDeleted { get; private set; }

    public async Task ExecuteAsync()
    {
        // Find record pointer
        var pointer = await primaryIndex.GetAsync(primaryKey);
        if (pointer == null)
        {
            WasDeleted = false;
            return; // Already deleted or never existed
        }
        
        // Remove from storage
        await dataSession.DeleteAsync(pointer);
        
        // Remove from primary index
        await primaryIndex.RemoveAsync(primaryKey);
        
        WasDeleted = true;
    }
}