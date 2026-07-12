namespace SharpDB.V2.Sql.Planning.Plans;
using SharpDB.V2.Sql.Planning;

public sealed class LimitPlan : QueryPlan
{
    public QueryPlan Source { get; init; } = null!;
    public int Count { get; init; }
}
