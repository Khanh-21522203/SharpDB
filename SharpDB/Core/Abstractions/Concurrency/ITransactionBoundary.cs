using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Concurrency;

public interface ITransactionBoundary
{
    ValueTask<ITransactionSession> BeginAsync(
        IsolationLevel isolation = IsolationLevel.ReadCommitted,
        CancellationToken ct = default);

    Task<TResult> ExecuteAsync<TResult>(
        Func<ITransactionSession, Task<TResult>> work,
        IsolationLevel isolation = IsolationLevel.ReadCommitted,
        CancellationToken ct = default);
}

public interface ITransactionSession : IAsyncDisposable
{
    long TransactionId { get; }

    ValueTask<byte[]?> ReadBytesAsync(Pointer pointer, CancellationToken ct = default);

    ValueTask<T?> ReadAsync<T>(Pointer pointer, CancellationToken ct = default)
        where T : class;

    ValueTask<Pointer> WriteBytesAsync(
        Pointer? pointer,
        byte[] value,
        int? collectionIdHint = null,
        CancellationToken ct = default);

    ValueTask<Pointer> WriteAsync<T>(Pointer? pointer, T value, CancellationToken ct = default)
        where T : class;

    ValueTask AcquireRangeReadLockAsync(
        string resourceType,
        string startInclusive,
        string endInclusive,
        CancellationToken ct = default);

    ValueTask AcquireRangeWriteLockAsync(
        string resourceType,
        string startInclusive,
        string endInclusive,
        CancellationToken ct = default);

    ValueTask RegisterRollbackActionAsync(
        Func<Task> rollbackAction,
        CancellationToken ct = default);

    ValueTask CommitAsync(CancellationToken ct = default);
    ValueTask RollbackAsync(CancellationToken ct = default);
}
