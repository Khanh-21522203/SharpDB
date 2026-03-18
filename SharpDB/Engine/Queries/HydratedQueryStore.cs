using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;
using SharpDB.Index.Manager;

namespace SharpDB.Engine.Queries;

public sealed class HydratedQueryStore<TKey, TRecord>(
    IUniqueTreeIndexManager<TKey, Pointer> primaryIndex,
    IUniqueQueryableIndex<TKey, Pointer> queryableIndex,
    IDataIOSession dataSession,
    IObjectSerializer serializer)
    : IHydratedQueryStore<TKey, TRecord>
    where TKey : IComparable<TKey>
    where TRecord : class
{
    public async Task<TRecord?> GetAsync(TKey key, CancellationToken ct = default)
    {
        var pointer = await primaryIndex.GetAsync(key);
        if (!IsValidDataPointer(pointer))
            return null;

        var dbObject = await dataSession.SelectAsync(pointer);
        if (dbObject == null)
            return null;

        return serializer.Deserialize<TRecord>(dbObject.Data);
    }

    public async IAsyncEnumerable<TRecord> QueryAsync(
        QuerySpec<TKey> spec,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        IAsyncEnumerable<KeyValue<TKey, Pointer>> keys = spec switch
        {
            QuerySpec<TKey>.Range range => queryableIndex.RangeAsync(range.MinInclusive, range.MaxInclusive),
            QuerySpec<TKey>.GreaterThan greaterThan => queryableIndex.GreaterThanAsync(greaterThan.Key),
            QuerySpec<TKey>.LessThan lessThan => queryableIndex.LessThanAsync(lessThan.Key),
            _ => throw new NotSupportedException($"Unsupported query spec: {spec.GetType().Name}")
        };

        await foreach (var keyValue in keys.WithCancellation(ct))
        {
            var dbObject = await dataSession.SelectAsync(keyValue.Value);
            if (dbObject == null)
                continue;

            yield return serializer.Deserialize<TRecord>(dbObject.Data);
        }
    }

    private static bool IsValidDataPointer(Pointer pointer)
    {
        return pointer.Type == Pointer.TypeData && pointer.Position > 0;
    }
}
