using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Schema;

public sealed class CollectionSchema
{
    public string Name { get; init; } = "";
    public PageId PrimaryIndexRootPageId { get; init; }
    public IReadOnlyList<IndexDefinition> SecondaryIndexDefinitions { get; init; } = [];
}
