using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Index;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.BTree;

public sealed class UniqueIndex<TKey> : IUniqueIndex<TKey>
{
    // IIndex<TKey>
    public string Name => throw new NotImplementedException();

    public bool ContainsKey(TKey key, IOperationContext ctx) => throw new NotImplementedException();

    public IIndexCursor<TKey, PageId> OpenCursor(IOperationContext ctx) => throw new NotImplementedException();

    public void Insert(TKey key, PageId value, IOperationContext ctx) { }

    public void Delete(TKey key, PageId value, IOperationContext ctx) { }

    // IUniqueIndex<TKey>
    public bool TryGet(TKey key, out PageId value, IOperationContext ctx)
    {
        value = default;
        return false;
    }

    public void Upsert(TKey key, PageId value, IOperationContext ctx) { }
}
