namespace SharpDB.Engine.Writes;

public interface ICollectionWriteOrchestrator<TRecord, in TKey>
    where TRecord : class
    where TKey : IComparable<TKey>
{
    ValueTask InsertAsync(TRecord record, CancellationToken ct = default);
    ValueTask<bool> UpdateAsync(TRecord record, bool upsert = false, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(TKey key, CancellationToken ct = default);
}
