namespace SharpDB.V2.Sql.Parsing.Ast;

public enum JoinType { Inner, Left, Right }

public sealed class JoinClause
{
    public JoinType Type { get; init; }
    public string TableName { get; init; } = "";
    public string? Alias { get; init; }
    public Expression OnExpression { get; init; } = null!;
}
