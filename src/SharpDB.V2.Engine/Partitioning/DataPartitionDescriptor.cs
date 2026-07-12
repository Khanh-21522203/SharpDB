using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Partitioning;

public sealed class DataPartitionDescriptor
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public byte[]? LowerBoundKey { get; init; }
    public byte[]? UpperBoundKey { get; init; }
    public PageId PrimaryIndexRootPageId { get; init; }
    public IReadOnlyDictionary<string, PageId> SecondaryIndexRootPageIds { get; init; } =
        new Dictionary<string, PageId>();
}
