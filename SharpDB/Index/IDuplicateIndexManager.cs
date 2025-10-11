namespace SharpDB.Index;

public interface IDuplicateIndexManager<TKey, TValue>
{
    bool TryAddIndex(TKey key, TValue value);
    bool TryGetIndex(TKey key, out List<TValue> values);
    bool TryRemoveIndex(TKey key, TValue value);
    bool TryRemoveAllIndexes(TKey key);
}