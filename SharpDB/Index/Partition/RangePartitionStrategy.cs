namespace SharpDB.Index.Partition;

/// <summary>
/// Routes keys to partitions based on sorted boundary values.
/// Example: boundaries=[100, 500] creates 3 partitions:
///   partition 0: (-∞, 100), partition 1: [100, 500), partition 2: [500, ∞)
/// Range queries only fan out to partitions whose ranges overlap [min, max].
/// </summary>
public class RangePartitionStrategy<TKey>(TKey[] boundaries) : IPartitionStrategy<TKey>
    where TKey : IComparable<TKey>
{
    /// <summary>Number of partitions = boundaries.Length + 1.</summary>
    public int PartitionCount => boundaries.Length + 1;

    public int GetPartition(TKey key, int partitionCount)
    {
        for (var i = 0; i < boundaries.Length; i++)
            if (key.CompareTo(boundaries[i]) < 0) return i;
        return partitionCount - 1;
    }

    public int[] GetPartitionsForRange(TKey min, TKey max, int partitionCount)
    {
        var start = GetPartition(min, partitionCount);
        var end = GetPartition(max, partitionCount);
        return Enumerable.Range(start, end - start + 1).ToArray();
    }
}
