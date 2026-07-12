namespace SharpDB.V2.Sql.Parsing.Ast;

public sealed class Literal : Expression
{
    public object? Value { get; init; }
}
