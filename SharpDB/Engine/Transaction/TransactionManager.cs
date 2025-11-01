using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Engine.Transaction;

public class TransactionManager : ITransactionManager
{
    private readonly ILockManager _lockManager;
    private readonly IObjectSerializer _serializer;
    private readonly IVersionManager _versionManager;
    private long _currentTimestamp = 1;
    private long _nextTxnId = 1;

    public TransactionManager(
        ILockManager lockManager,
        IVersionManager versionManager,
        IObjectSerializer serializer)
    {
        _lockManager = lockManager;
        _versionManager = versionManager;
        _serializer = serializer;
    }

    public Task<ITransaction> BeginTransactionAsync(IsolationLevel level = IsolationLevel.ReadCommitted)
    {
        var txnId = Interlocked.Increment(ref _nextTxnId);
        var timestamp = Interlocked.Increment(ref _currentTimestamp);

        ITransaction txn = new Transaction(
            txnId,
            timestamp,
            level,
            _lockManager,
            _versionManager,
            _serializer
        );

        return Task.FromResult(txn);
    }

    public async Task CommitAsync(ITransaction transaction)
    {
        var commitTimestamp = Interlocked.Increment(ref _currentTimestamp);

        await _versionManager.CommitAsync(transaction.TransactionId, commitTimestamp);
        await _lockManager.ReleaseAllLocksAsync(new TransactionId(transaction.TransactionId));
    }

    public async Task RollbackAsync(ITransaction transaction)
    {
        await _versionManager.AbortAsync(transaction.TransactionId);
        await _lockManager.ReleaseAllLocksAsync(new TransactionId(transaction.TransactionId));
    }

    public void Dispose()
    {
        _lockManager?.Dispose();
        _versionManager?.Dispose();
    }
}