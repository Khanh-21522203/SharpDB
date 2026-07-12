using SharpDB.V2.Engine.Abstractions.Partitioning;

namespace SharpDB.V2.Engine.Partitioning;

public sealed class PartitionRouter<TKey>
{
    private readonly IPartitionStrategy<TKey> _strategy;
    private readonly IReadOnlyList<DataPartitionDescriptor> _partitions;

    public PartitionRouter(IPartitionStrategy<TKey> strategy, IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        _strategy = strategy;
        _partitions = partitions;
    }

    public DataPartitionDescriptor Route(TKey key)
    {
        var index = _strategy.GetPartition(key, _partitions);
        return _partitions[index];
    }

    public IReadOnlyList<DataPartitionDescriptor> RouteRange(TKey from, TKey to) =>
        RouteRange(PartitionRange<TKey>.ClosedOpen(from, to));

    public IReadOnlyList<DataPartitionDescriptor> RouteRange(PartitionRange<TKey> range)
    {
        var indexes = _strategy.GetPartitionsForRange(range, _partitions);
        var result = new DataPartitionDescriptor[indexes.Count];
        for (var i = 0; i < indexes.Count; i++)
        {
            result[i] = _partitions[indexes[i]];
        }

        return result;
    }
}
