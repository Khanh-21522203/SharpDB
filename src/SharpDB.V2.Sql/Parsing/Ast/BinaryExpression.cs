namespace SharpDB.V2.Sql.Parsing.Ast;

public enum BinaryOperator { Eq, Neq, Lt, Gt, Lte, Gte, And, Or }

public sealed class BinaryExpression : Expression
{
    public Expression Left { get; init; } = null!;
    public BinaryOperator Operator { get; init; }
    public Expression Right { get; init; } = null!;
}
