namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class DeleteStatement : Statement
{
    public string TableName { get; init; } = "";
    public WhereClause? Where { get; init; }
}
