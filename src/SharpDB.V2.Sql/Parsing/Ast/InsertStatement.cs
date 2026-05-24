namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class InsertStatement : Statement
{
    public string TableName { get; init; } = "";
    public IReadOnlyList<string> Columns { get; init; } = [];
    public IReadOnlyList<Expression> Values { get; init; } = [];
}
