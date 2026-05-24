namespace SharpDB.V2.Sql.Planning.Plans;
using SharpDB.V2.Sql.Planning;
using SharpDB.V2.Sql.Parsing.Ast;

public sealed class JoinPlan : QueryPlan
{
    public QueryPlan Left { get; init; } = null!;
    public QueryPlan Right { get; init; } = null!;
    public JoinType JoinType { get; init; }
    public Expression OnExpression { get; init; } = null!;
}
