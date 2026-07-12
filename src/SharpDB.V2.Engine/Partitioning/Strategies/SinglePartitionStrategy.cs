using SharpDB.V2.Engine.Abstractions.Partitioning;

namespace SharpDB.V2.Engine.Partitioning.Strategies;

public sealed class SinglePartitionStrategy<TKey> : IPartitionStrategy<TKey>
{
    public int GetPartition(TKey key, IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        ValidateSinglePartition(partitions);
        return 0;
    }

    public IReadOnlyList<int> GetPartitionsForRange(
        PartitionRange<TKey> range,
        IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        ValidateSinglePartition(partitions);
        return [0];
    }

    private static void ValidateSinglePartition(IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        if (partitions.Count != 1)
        {
            throw new ArgumentException("Single-partition routing requires exactly one partition.", nameof(partitions));
        }
    }
}
