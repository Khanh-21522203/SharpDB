using SharpDB.V2.Engine.Abstractions.Storage;
using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Abstractions.Wal;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Wal;

public sealed class RecoveryManager : IRecoveryManager
{
    public RecoveryResult Recover(ILogWriter log, IPageStore store) => throw new NotImplementedException();

    public LogSequenceNumber FindCheckpoint(ILogWriter log) => throw new NotImplementedException();

    public void Checkpoint(ILogWriter log, IPageStore store, ITransactionManager txm) { }
}
