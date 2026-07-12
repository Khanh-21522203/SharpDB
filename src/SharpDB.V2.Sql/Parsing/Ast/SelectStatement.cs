namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class SelectStatement : Statement
{
    public IReadOnlyList<string> Columns { get; init; } = [];
    public string TableName { get; init; } = "";
    public IReadOnlyList<JoinClause> Joins { get; init; } = [];
    public WhereClause? Where { get; init; }
    public IReadOnlyList<OrderByClause> OrderBy { get; init; } = [];
    public int? Limit { get; init; }
}
