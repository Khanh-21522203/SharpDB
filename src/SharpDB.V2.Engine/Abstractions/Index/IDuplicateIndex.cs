using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Index;

public interface IDuplicateIndex<TKey> : IIndex<TKey>
{
    IIndexCursor<TKey, PageId> OpenRangeCursor(TKey from, TKey to, IOperationContext ctx);
    int CountDuplicates(TKey key, IOperationContext ctx);
}
