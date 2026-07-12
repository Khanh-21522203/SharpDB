using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Index;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.BTree;

public sealed class DuplicateIndex<TKey> : IDuplicateIndex<TKey>
{
    // IIndex<TKey>
    public string Name => throw new NotImplementedException();

    public bool ContainsKey(TKey key, IOperationContext ctx) => throw new NotImplementedException();

    public IIndexCursor<TKey, PageId> OpenCursor(IOperationContext ctx) => throw new NotImplementedException();

    public void Insert(TKey key, PageId value, IOperationContext ctx) { }

    public void Delete(TKey key, PageId value, IOperationContext ctx) { }

    // IDuplicateIndex<TKey>
    public IIndexCursor<TKey, PageId> OpenRangeCursor(TKey from, TKey to, IOperationContext ctx) => throw new NotImplementedException();

    public int CountDuplicates(TKey key, IOperationContext ctx) => throw new NotImplementedException();
}
