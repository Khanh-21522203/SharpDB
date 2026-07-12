using SharpDB.V2.Engine.Partitioning;

namespace SharpDB.V2.Engine.Schema;

/// <summary>
/// Durable catalog entry for one logical collection/table. It describes the row layout, the base
/// data partitions that own full rows, and the independent index partitions used by global indexes.
///
/// <code>
/// CollectionSchema
/// ├── Columns[]                         row schema used by schema-bound BinarySerializer&lt;T&gt;
/// ├── PrimaryKeyFieldPath               logical row identity
/// ├── BasePartitionKeyPaths[]           fields used to route full row data; empty means primary key
/// ├── Partitions[]                      base/data partitions
/// │   ├── PrimaryIndexRootPageId         local primary B+Tree root for this data partition
/// │   └── SecondaryIndexRootPageIds      local secondary roots: index name -&gt; root page
/// ├── SecondaryIndexDefinitions[]       logical index definitions
/// │   ├── Scope = Local                  roots live inside each data partition above
/// │   └── Scope = Global                 roots live in IndexPartitionDescriptor entries below
/// └── GlobalIndexPartitions[]           physical partitions for independent/global index B+Trees
///     ├── IndexName                      links back to one global IndexDefinition
///     └── RootPageId                     B+Tree root for this index partition
/// </code>
/// </summary>
public sealed class CollectionSchema
{
    public string Name { get; init; } = "";
    public int SchemaVersion { get; init; } = 1;
    public string PrimaryKeyFieldPath { get; init; } = "";
    public IReadOnlyList<ColumnDefinition> Columns { get; init; } = [];
    public PartitionKind PartitionKind { get; init; } = PartitionKind.None;
    public IReadOnlyList<string> BasePartitionKeyPaths { get; init; } = [];
    public IReadOnlyList<IndexDefinition> SecondaryIndexDefinitions { get; init; } = [];
    public IReadOnlyList<DataPartitionDescriptor> Partitions { get; init; } = [];
    public IReadOnlyList<IndexPartitionDescriptor> GlobalIndexPartitions { get; init; } = [];
}
