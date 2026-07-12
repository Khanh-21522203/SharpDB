using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Transactions;

public interface ITransaction : IDisposable
{
    TransactionId Id { get; }
    TransactionStatus Status { get; }
    void Commit();
    void Rollback();
}
