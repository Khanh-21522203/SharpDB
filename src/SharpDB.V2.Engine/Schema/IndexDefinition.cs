namespace SharpDB.V2.Engine.Schema;

public sealed class IndexDefinition
{
    public string Name { get; init; } = "";
    public string FieldPath { get; init; } = "";
    public bool IsUnique { get; init; }
    public bool Ascending { get; init; } = true;
}
