using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Transactions;

public sealed class LockManager : ILockManager
{
    public LockResult TryAcquire(LockKey key, LockMode mode, ITransaction tx, TimeSpan timeout) => throw new NotImplementedException();

    public void Release(ITransaction tx) { }

    public bool DetectDeadlock(ITransaction tx, out ITransaction victim)
    {
        victim = default!;
        return false;
    }
}
