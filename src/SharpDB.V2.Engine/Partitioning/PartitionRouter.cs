using SharpDB.V2.Engine.Abstractions.Partitioning;

namespace SharpDB.V2.Engine.Partitioning;

public sealed class PartitionRouter<TKey>
{
    private readonly IPartitionStrategy<TKey> _strategy;
    private readonly IReadOnlyList<PartitionDescriptor> _partitions;

    public PartitionRouter(IPartitionStrategy<TKey> strategy, IReadOnlyList<PartitionDescriptor> partitions)
    {
        _strategy = strategy;
        _partitions = partitions;
    }

    public PartitionDescriptor Route(TKey key)
    {
        var index = _strategy.GetPartition(key, _partitions.Count);
        return _partitions[index];
    }
}
