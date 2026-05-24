using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Transactions;

public interface ILockManager
{
    LockResult TryAcquire(LockKey key, LockMode mode, ITransaction tx, TimeSpan timeout);
    void Release(ITransaction tx);
    bool DetectDeadlock(ITransaction tx, out ITransaction victim);
}
