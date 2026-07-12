namespace SharpDB.V2.Sql.Execution;

public sealed class ResultSet
{
    public IReadOnlyList<string> Columns { get; init; } = [];
    public IReadOnlyList<Row> Rows { get; init; } = [];
}
