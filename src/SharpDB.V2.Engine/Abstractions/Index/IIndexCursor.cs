namespace SharpDB.V2.Engine.Abstractions.Index;

public interface IIndexCursor<TKey, TValue> : IDisposable
{
    bool MoveNext();
    bool MovePrev();
    TKey CurrentKey { get; }
    TValue CurrentValue { get; }
    void SeekTo(TKey key);
    void SeekToFirst();
    void SeekToLast();
}
