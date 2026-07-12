namespace SharpDB.V2.Sql.Execution;

public sealed class Row
{
    public IReadOnlyList<object?> Values { get; init; } = [];
}
