namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class UpdateStatement : Statement
{
    public string TableName { get; init; } = "";
    public IReadOnlyList<(string Column, Expression Value)> Assignments { get; init; } = [];
    public WhereClause? Where { get; init; }
}
