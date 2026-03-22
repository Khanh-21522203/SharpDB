namespace SharpDB.Index.Partition;

public interface IPartitionStrategy<TKey>
{
    int GetPartition(TKey key, int partitionCount);
    int[] GetPartitionsForRange(TKey min, TKey max, int partitionCount);
}
