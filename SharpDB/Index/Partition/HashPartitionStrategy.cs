namespace SharpDB.Index.Partition;

public class HashPartitionStrategy<TKey> : IPartitionStrategy<TKey>
{
    public int GetPartition(TKey key, int partitionCount)
    {
        return key switch
        {
            long l => (int)(Math.Abs(l) % partitionCount),
            int i  => Math.Abs(i) % partitionCount,
            _      => Math.Abs(key!.GetHashCode()) % partitionCount
        };
    }

    // Hash partitioning cannot prune partitions for range queries — all partitions may contain matching keys.
    public int[] GetPartitionsForRange(TKey min, TKey max, int partitionCount)
        => Enumerable.Range(0, partitionCount).ToArray();
}
