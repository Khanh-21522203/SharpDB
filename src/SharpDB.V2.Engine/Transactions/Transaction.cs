using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Transactions;

public sealed class Transaction : ITransaction
{
    public TransactionId Id => throw new NotImplementedException();

    public TransactionStatus Status => throw new NotImplementedException();

    public void Commit() { }

    public void Rollback() { }

    public void Dispose() { }
}
