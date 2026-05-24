namespace SharpDB.V2.Engine.Types;

public sealed class VacuumResult
{
    public long PagesReclaimed { get; init; }
    public long BytesFreed { get; init; }
}
