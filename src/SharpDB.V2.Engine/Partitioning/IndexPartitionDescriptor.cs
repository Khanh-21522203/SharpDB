using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Partitioning;

public sealed class IndexPartitionDescriptor
{
    public string IndexName { get; init; } = "";
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public byte[]? LowerBoundKey { get; init; }
    public byte[]? UpperBoundKey { get; init; }
    public PageId RootPageId { get; init; }
}
