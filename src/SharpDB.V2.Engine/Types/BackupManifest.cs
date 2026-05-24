namespace SharpDB.V2.Engine.Types;

public sealed class BackupManifest
{
    public string SourcePath { get; init; } = "";
    public DateTimeOffset CreatedAt { get; init; }
    public long TotalPages { get; init; }
    public LogSequenceNumber ConsistentLsn { get; init; }
}
