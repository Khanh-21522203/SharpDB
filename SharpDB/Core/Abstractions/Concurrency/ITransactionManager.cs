using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Concurrency;

public interface ITransactionManager : IDisposable
{
    Task<ITransaction> BeginTransactionAsync(IsolationLevel level = IsolationLevel.ReadCommitted);
    Task CommitAsync(ITransaction transaction);
    Task RollbackAsync(ITransaction transaction);
}

public enum IsolationLevel
{
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable
}

public interface ITransaction : IDisposable
{
    long TransactionId { get; }
    long StartTimestamp { get; }
    IsolationLevel IsolationLevel { get; }
    Task<T?> ReadAsync<T>(Pointer pointer) where T : class;
    Task<Pointer> WriteAsync<T>(Pointer? pointer, T data) where T : class;
}