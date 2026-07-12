namespace SharpDB.V2.Engine.Schema;

/// <summary>Describes where an index's physical B+Tree roots live relative to base data partitions.</summary>
public enum IndexScope
{
    /// <summary>The index is colocated with each base data partition and its roots live in DataPartitionDescriptor.</summary>
    Local,
    /// <summary>The index has independent index partitions and its roots live in IndexPartitionDescriptor.</summary>
    Global
}

public sealed class IndexFieldDefinition
{
    public string FieldPath { get; init; } = "";
    public bool Ascending { get; init; } = true;
}

public sealed class IndexDefinition
{
    public string Name { get; init; } = "";
    public bool IsUnique { get; init; }
    public IndexScope Scope { get; init; } = IndexScope.Local;
    public IReadOnlyList<string> PartitionKeyPaths { get; init; } = [];
    public IReadOnlyList<IndexFieldDefinition> Fields { get; init; } = [];
}
