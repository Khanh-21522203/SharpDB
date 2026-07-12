using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Transactions;

public sealed class TransactionManager : ITransactionManager
{
    public ITransaction Begin(IsolationLevel level = IsolationLevel.ReadCommitted) => throw new NotImplementedException();

    public ITransaction? GetActive(TransactionId id) => throw new NotImplementedException();

    public IReadOnlyList<ITransaction> GetActiveTransactions() => throw new NotImplementedException();
}
