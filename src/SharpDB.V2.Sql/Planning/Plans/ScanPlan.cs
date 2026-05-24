namespace SharpDB.V2.Sql.Planning.Plans;
using SharpDB.V2.Sql.Planning;

public sealed class ScanPlan : QueryPlan
{
    public string TableName { get; init; } = "";
}
