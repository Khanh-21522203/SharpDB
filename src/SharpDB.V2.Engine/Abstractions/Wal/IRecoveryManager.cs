using SharpDB.V2.Engine.Abstractions.Storage;
using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Wal;

public interface IRecoveryManager
{
    RecoveryResult Recover(ILogWriter log, IPageStore store);
    LogSequenceNumber FindCheckpoint(ILogWriter log);
    void Checkpoint(ILogWriter log, IPageStore store, ITransactionManager txm);
}
