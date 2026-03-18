namespace SharpDB.Engine.Queries;

public interface IHydratedQueryStore<TKey, TRecord>
    where TKey : IComparable<TKey>
{
    Task<TRecord?> GetAsync(TKey key, CancellationToken ct = default);
    IAsyncEnumerable<TRecord> QueryAsync(QuerySpec<TKey> spec, CancellationToken ct = default);
}
