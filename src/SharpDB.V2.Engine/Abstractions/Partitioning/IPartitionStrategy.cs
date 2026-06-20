namespace SharpDB.V2.Engine.Abstractions.Partitioning;

public interface IPartitionStrategy<TKey>
{
    int GetPartition(TKey key, int partitionCount);
}
