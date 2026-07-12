namespace SharpDB.V2.Sql.Planning.Plans;
using SharpDB.V2.Sql.Planning;
using SharpDB.V2.Sql.Parsing.Ast;

public sealed class FilterPlan : QueryPlan
{
    public QueryPlan Source { get; init; } = null!;
    public Expression Predicate { get; init; } = null!;
}
