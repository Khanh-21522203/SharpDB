using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Transactions;

public interface ITransactionManager
{
    ITransaction Begin(IsolationLevel level = IsolationLevel.ReadCommitted);
    ITransaction? GetActive(TransactionId id);
    IReadOnlyList<ITransaction> GetActiveTransactions();
}
