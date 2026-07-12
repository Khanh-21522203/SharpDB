namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class ColumnRef : Expression
{
    public string? TableAlias { get; init; }
    public string ColumnName { get; init; } = "";
}
