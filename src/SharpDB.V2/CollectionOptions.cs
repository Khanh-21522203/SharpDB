namespace SharpDB.V2;
using SharpDB.V2.Engine.Abstractions.Partitioning;
using SharpDB.V2.Engine.Partitioning;

public sealed class CollectionOptions<T, TKey>
{
    public Func<T, TKey>? KeySelector { get; init; }
    public bool AutoIncrement { get; init; }
    public PartitionKind PartitionKind { get; init; } = PartitionKind.None;
    public int PartitionCount { get; init; } = 1;
    public IPartitionStrategy<TKey>? PartitionStrategy { get; init; }
}
