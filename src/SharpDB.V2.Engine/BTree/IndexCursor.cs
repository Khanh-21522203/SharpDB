using SharpDB.V2.Engine.Abstractions.Index;

namespace SharpDB.V2.Engine.BTree;

public sealed class IndexCursor<TKey, TValue> : IIndexCursor<TKey, TValue>
{
    public bool MoveNext() => throw new NotImplementedException();

    public bool MovePrev() => throw new NotImplementedException();

    public TKey CurrentKey => throw new NotImplementedException();

    public TValue CurrentValue => throw new NotImplementedException();

    public void SeekTo(TKey key) { }

    public void SeekToFirst() { }

    public void SeekToLast() { }

    public void Dispose() { }
}
