using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Wal;

public interface ILogWriter : IDisposable
{
    LogSequenceNumber CurrentLsn { get; }
    LogSequenceNumber Append(scoped in ReadOnlySpan<byte> record);
    ValueTask<LogSequenceNumber> AppendAsync(scoped in ReadOnlySpan<byte> record);
    void ForceSync(LogSequenceNumber upTo);
    void ScheduleGroupCommit(LogSequenceNumber lsn, Action<LogSequenceNumber> onFlushed);
}
