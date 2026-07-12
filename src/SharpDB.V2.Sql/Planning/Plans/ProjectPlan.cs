namespace SharpDB.V2.Sql.Planning.Plans;
using SharpDB.V2.Sql.Planning;

public sealed class ProjectPlan : QueryPlan
{
    public QueryPlan Source { get; init; } = null!;
    public IReadOnlyList<string> Columns { get; init; } = [];
}
