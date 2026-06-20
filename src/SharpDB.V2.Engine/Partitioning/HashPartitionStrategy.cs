using SharpDB.V2.Engine.Abstractions.Partitioning;

namespace SharpDB.V2.Engine.Partitioning;

public sealed class HashPartitionStrategy<TKey> : IPartitionStrategy<TKey>
{
    public int GetPartition(TKey key, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount));
        }

        var hash = key?.GetHashCode() ?? 0;
        return (hash & int.MaxValue) % partitionCount;
    }
}
