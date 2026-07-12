namespace SharpDB.V2.Tests.Helpers;
using SharpDB.V2.Engine.Abstractions.Wal;
using SharpDB.V2.Engine.Types;

public sealed class FakeLogWriter : ILogWriter
{
    public LogSequenceNumber CurrentLsn => new(0);
    public LogSequenceNumber Append(scoped in ReadOnlySpan<byte> record) => new(0);
    public ValueTask<LogSequenceNumber> AppendAsync(scoped in ReadOnlySpan<byte> record) => ValueTask.FromResult(new LogSequenceNumber(0));
    public void ForceSync(LogSequenceNumber upTo) { }
    public void ScheduleGroupCommit(LogSequenceNumber lsn, Action<LogSequenceNumber> onFlushed) { }
    public void Dispose() { }
}
