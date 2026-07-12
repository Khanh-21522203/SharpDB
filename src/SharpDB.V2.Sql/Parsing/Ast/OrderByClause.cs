namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class OrderByClause
{
    public Expression Expression { get; init; } = null!;
    public bool Ascending { get; init; } = true;
}
