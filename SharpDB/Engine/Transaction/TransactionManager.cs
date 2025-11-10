using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.WAL;

namespace SharpDB.Engine.Transaction;

public class TransactionManager : ITransactionManager
{
    private readonly ILockManager _lockManager;
    private readonly IObjectSerializer _serializer;
    private readonly IVersionManager _versionManager;
    private readonly WALManager? _walManager;
    private long _currentTimestamp = 1;
    private long _nextTxnId = 1;

    public TransactionManager(
        ILockManager lockManager,
        IVersionManager versionManager,
        IObjectSerializer serializer,
        WALManager? walManager = null)
    {
        _lockManager = lockManager;
        _versionManager = versionManager;
        _serializer = serializer;
        _walManager = walManager;
    }

    public Task<ITransaction> BeginTransactionAsync(IsolationLevel level = IsolationLevel.ReadCommitted)
    {
        var txnId = Interlocked.Increment(ref _nextTxnId);
        var timestamp = Interlocked.Increment(ref _currentTimestamp);

        // Log transaction begin to WAL
        _walManager?.LogTransactionBegin(txnId);

        ITransaction txn = new Transaction(
            txnId,
            timestamp,
            level,
            _lockManager,
            _versionManager,
            _serializer,
            _walManager
        );

        return Task.FromResult(txn);
    }

    public async Task CommitAsync(ITransaction transaction)
    {
        var commitTimestamp = Interlocked.Increment(ref _currentTimestamp);

        // Log commit to WAL before making changes permanent
        _walManager?.LogTransactionCommit(transaction.TransactionId);
        _walManager?.Flush();

        await _versionManager.CommitAsync(transaction.TransactionId, commitTimestamp);
        await _lockManager.ReleaseAllLocksAsync(new TransactionId(transaction.TransactionId));
    }

    public async Task RollbackAsync(ITransaction transaction)
    {
        // Log abort to WAL
        _walManager?.LogTransactionAbort(transaction.TransactionId);
        
        await _versionManager.AbortAsync(transaction.TransactionId);
        await _lockManager.ReleaseAllLocksAsync(new TransactionId(transaction.TransactionId));
    }

    public void Dispose()
    {
        _lockManager?.Dispose();
        _versionManager?.Dispose();
    }
}