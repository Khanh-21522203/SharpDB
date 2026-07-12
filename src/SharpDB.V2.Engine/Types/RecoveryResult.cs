namespace SharpDB.V2.Engine.Types;

public sealed class RecoveryResult
{
    public bool WasCleanShutdown { get; init; }
    public int PagesRedone { get; init; }
    public int TransactionsUndone { get; init; }
    public LogSequenceNumber CheckpointLsn { get; init; }
}
