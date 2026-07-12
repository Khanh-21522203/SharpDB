using SharpDB.V2.Engine.Abstractions.Wal;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Wal;

public sealed class LogWriter : ILogWriter
{
    public LogSequenceNumber CurrentLsn => throw new NotImplementedException();

    public LogSequenceNumber Append(scoped in ReadOnlySpan<byte> record) => throw new NotImplementedException();

    public ValueTask<LogSequenceNumber> AppendAsync(scoped in ReadOnlySpan<byte> record) => throw new NotImplementedException();

    public void ForceSync(LogSequenceNumber upTo) { }

    public void ScheduleGroupCommit(LogSequenceNumber lsn, Action<LogSequenceNumber> onFlushed) { }

    public void Dispose() { }
}
