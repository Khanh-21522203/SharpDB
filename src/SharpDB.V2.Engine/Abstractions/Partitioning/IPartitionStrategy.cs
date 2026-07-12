using SharpDB.V2.Engine.Partitioning;

namespace SharpDB.V2.Engine.Abstractions.Partitioning;

public interface IPartitionStrategy<TKey>
{
    int GetPartition(TKey key, IReadOnlyList<DataPartitionDescriptor> partitions);
    IReadOnlyList<int> GetPartitionsForRange(
        PartitionRange<TKey> range,
        IReadOnlyList<DataPartitionDescriptor> partitions);
}
