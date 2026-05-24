using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Index;

public interface IIndex<TKey>
{
    string Name { get; }
    bool ContainsKey(TKey key, IOperationContext ctx);
    IIndexCursor<TKey, PageId> OpenCursor(IOperationContext ctx);
    void Insert(TKey key, PageId value, IOperationContext ctx);
    void Delete(TKey key, PageId value, IOperationContext ctx);
}
