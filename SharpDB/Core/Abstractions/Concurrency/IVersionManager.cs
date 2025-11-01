using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Concurrency;

public interface IVersionManager : IDisposable
{
    Task<VersionedRecord?> ReadAsync(Pointer pointer, long readTimestamp);
    Task<Pointer> WriteAsync(Pointer? pointer, byte[] data, long writeTimestamp, long txnId);
    Task CommitAsync(long txnId, long commitTimestamp);
    Task AbortAsync(long txnId);
    Task GarbageCollectAsync(long minActiveTimestamp);
}

public class VersionedRecord
{
    public Pointer Pointer { get; set; }
    public byte[] Data { get; set; } = [];
    public long BeginTimestamp { get; set; }
    public long EndTimestamp { get; set; } = long.MaxValue;
    public long TransactionId { get; set; }
    public Pointer? PreviousVersion { get; set; }
    public bool IsCommitted { get; set; }
}