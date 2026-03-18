using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.DataStructures;

namespace SharpDB.Engine.Transaction;

public sealed class TransactionBoundary(ITransactionManager transactionManager) : ITransactionBoundary
{
    public async ValueTask<ITransactionSession> BeginAsync(
        IsolationLevel isolation = IsolationLevel.ReadCommitted,
        CancellationToken ct = default)
    {
        var transaction = await transactionManager.BeginTransactionAsync(isolation);
        return new TransactionSession(transactionManager, transaction);
    }

    public async Task<TResult> ExecuteAsync<TResult>(
        Func<ITransactionSession, Task<TResult>> work,
        IsolationLevel isolation = IsolationLevel.ReadCommitted,
        CancellationToken ct = default)
    {
        await using var session = await BeginAsync(isolation, ct);

        try
        {
            var result = await work(session);
            await session.CommitAsync(ct);
            return result;
        }
        catch
        {
            await session.RollbackAsync(ct);
            throw;
        }
    }

    private sealed class TransactionSession(ITransactionManager manager, ITransaction transaction) : ITransactionSession
    {
        private bool _completed;

        public long TransactionId => transaction.TransactionId;

        public async ValueTask<T?> ReadAsync<T>(Pointer pointer, CancellationToken ct = default) where T : class
        {
            return await transaction.ReadAsync<T>(pointer);
        }

        public async ValueTask<byte[]?> ReadBytesAsync(Pointer pointer, CancellationToken ct = default)
        {
            return await transaction.ReadBytesAsync(pointer);
        }

        public async ValueTask<Pointer> WriteAsync<T>(Pointer? pointer, T value, CancellationToken ct = default)
            where T : class
        {
            return await transaction.WriteAsync(pointer, value);
        }

        public async ValueTask<Pointer> WriteBytesAsync(
            Pointer? pointer,
            byte[] value,
            int? collectionIdHint = null,
            CancellationToken ct = default)
        {
            return await transaction.WriteBytesAsync(pointer, value, collectionIdHint);
        }

        public async ValueTask AcquireRangeReadLockAsync(
            string resourceType,
            string startInclusive,
            string endInclusive,
            CancellationToken ct = default)
        {
            await transaction.AcquireRangeReadLockAsync(resourceType, startInclusive, endInclusive);
        }

        public async ValueTask AcquireRangeWriteLockAsync(
            string resourceType,
            string startInclusive,
            string endInclusive,
            CancellationToken ct = default)
        {
            await transaction.AcquireRangeWriteLockAsync(resourceType, startInclusive, endInclusive);
        }

        public async ValueTask RegisterRollbackActionAsync(
            Func<Task> rollbackAction,
            CancellationToken ct = default)
        {
            await transaction.RegisterRollbackActionAsync(rollbackAction);
        }

        public async ValueTask CommitAsync(CancellationToken ct = default)
        {
            if (_completed)
                return;

            await manager.CommitAsync(transaction);
            _completed = true;
        }

        public async ValueTask RollbackAsync(CancellationToken ct = default)
        {
            if (_completed)
                return;

            await manager.RollbackAsync(transaction);
            _completed = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (!_completed)
                await RollbackAsync();

            transaction.Dispose();
        }
    }
}
