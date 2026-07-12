namespace SharpDB.V2.Engine.Schema;

public sealed class ColumnDefinition
{
    public string Name { get; init; } = "";
    public string FieldPath { get; init; } = "";
    public string TypeName { get; init; } = "";
    public bool IsNullable { get; init; }
    public bool IsPrimaryKey { get; init; }
}
