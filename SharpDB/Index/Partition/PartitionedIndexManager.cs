using SharpDB.DataStructures;
using SharpDB.Index.Manager;

namespace SharpDB.Index.Partition;

public class PartitionedIndexManager<TKey>(
    IUniqueQueryableIndex<TKey, Pointer>[] partitions,
    IPartitionStrategy<TKey> strategy)
    : IUniqueQueryableIndex<TKey, Pointer>
    where TKey : IComparable<TKey>
{
    private readonly int _partitionCount = partitions.Length;

    // --- Point operations: route to owning partition ---

    public Task<Pointer> GetAsync(TKey key)
        => partitions[GetPartition(key)].GetAsync(key)!;

    public Task PutAsync(TKey key, Pointer value)
        => partitions[GetPartition(key)].PutAsync(key, value);

    public Task<bool> RemoveAsync(TKey key)
        => partitions[GetPartition(key)].RemoveAsync(key);

    public Task<bool> ContainsKeyAsync(TKey key)
        => partitions[GetPartition(key)].ContainsKeyAsync(key);

    // --- Aggregate operations: fan out to all partitions ---

    public async Task<int> CountAsync()
    {
        var counts = await Task.WhenAll(partitions.Select(p => p.CountAsync()));
        return counts.Sum();
    }

    public async Task FlushAsync()
        => await Task.WhenAll(partitions.Select(p => p.FlushAsync()));

    // --- Range operations: fan out to relevant partitions only, collect, sort by key ---

    public async IAsyncEnumerable<KeyValue<TKey, Pointer>> RangeAsync(TKey minKey, TKey maxKey)
    {
        var relevant = strategy.GetPartitionsForRange(minKey, maxKey, _partitionCount);
        var results = await FanOutAsync(relevant, p => p.RangeAsync(minKey, maxKey));
        foreach (var kv in results.OrderBy(kv => kv.Key))
            yield return kv;
    }

    public async IAsyncEnumerable<KeyValue<TKey, Pointer>> GreaterThanAsync(TKey key)
    {
        var relevant = strategy.GetPartitionsForRange(key, GetMaxKey(), _partitionCount);
        var results = await FanOutAsync(relevant, p => p.GreaterThanAsync(key));
        foreach (var kv in results.OrderBy(kv => kv.Key))
            yield return kv;
    }

    public async IAsyncEnumerable<KeyValue<TKey, Pointer>> LessThanAsync(TKey key)
    {
        var relevant = strategy.GetPartitionsForRange(GetMinKey(), key, _partitionCount);
        var results = await FanOutAsync(relevant, p => p.LessThanAsync(key));
        foreach (var kv in results.OrderBy(kv => kv.Key))
            yield return kv;
    }

    public void Dispose()
    {
        foreach (var partition in partitions)
            partition.Dispose();
    }

    // --- Helpers ---

    private int GetPartition(TKey key) => strategy.GetPartition(key, _partitionCount);

    private async Task<List<KeyValue<TKey, Pointer>>> FanOutAsync(
        int[] partitionIndexes,
        Func<IUniqueQueryableIndex<TKey, Pointer>, IAsyncEnumerable<KeyValue<TKey, Pointer>>> query)
    {
        var tasks = partitionIndexes.Select(i => CollectAsync(query(partitions[i]))).ToArray();
        var batches = await Task.WhenAll(tasks);
        return batches.SelectMany(b => b).ToList();
    }

    private static async Task<List<KeyValue<TKey, Pointer>>> CollectAsync(
        IAsyncEnumerable<KeyValue<TKey, Pointer>> source)
    {
        var results = new List<KeyValue<TKey, Pointer>>();
        await foreach (var kv in source)
            results.Add(kv);
        return results;
    }

    // Sentinel keys used to compute partition ranges for GreaterThan/LessThan queries.
    // These are type-safe extremes that the strategy maps to partition 0 or partitionCount-1.
    private static TKey GetMinKey()
    {
        var type = typeof(TKey);
        if (type == typeof(long)) return (TKey)(object)long.MinValue;
        if (type == typeof(int)) return (TKey)(object)int.MinValue;
        if (type == typeof(string)) return (TKey)(object)string.Empty;
        if (type == typeof(DateTime)) return (TKey)(object)DateTime.MinValue;
        if (type == typeof(decimal)) return (TKey)(object)decimal.MinValue;
        if (type == typeof(Guid)) return (TKey)(object)Guid.Empty;
        return default!;
    }

    private static TKey GetMaxKey()
    {
        var type = typeof(TKey);
        if (type == typeof(long)) return (TKey)(object)long.MaxValue;
        if (type == typeof(int)) return (TKey)(object)int.MaxValue;
        if (type == typeof(string)) return (TKey)(object)new string('\uffff', 255);
        if (type == typeof(DateTime)) return (TKey)(object)DateTime.MaxValue;
        if (type == typeof(decimal)) return (TKey)(object)decimal.MaxValue;
        if (type == typeof(Guid)) return (TKey)(object)new Guid(new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 });
        return default!;
    }
}
