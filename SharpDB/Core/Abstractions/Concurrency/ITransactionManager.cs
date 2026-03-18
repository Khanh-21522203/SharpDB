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
    Task<byte[]?> ReadBytesAsync(Pointer pointer);
    Task<T?> ReadAsync<T>(Pointer pointer) where T : class;
    Task<Pointer> WriteBytesAsync(Pointer? pointer, byte[] data, int? collectionIdHint = null);
    Task<Pointer> WriteAsync<T>(Pointer? pointer, T data) where T : class;
    Task AcquireRangeReadLockAsync(string resourceType, string startInclusive, string endInclusive);
    Task AcquireRangeWriteLockAsync(string resourceType, string startInclusive, string endInclusive);
    Task RegisterRollbackActionAsync(Func<Task> rollbackAction);
    Task RunRollbackActionsAsync();
}
