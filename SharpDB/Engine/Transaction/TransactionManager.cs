using Serilog;
using SharpDB.Core.Abstractions.Concurrency;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.WAL;

namespace SharpDB.Engine.Transaction;

public class TransactionManager : ITransactionManager
{
    private readonly ILockManager _lockManager;
    private readonly ILogger _logger = Log.ForContext<TransactionManager>();
    private readonly IObjectSerializer _serializer;
    private readonly IVersionManager _versionManager;
    private readonly WALManager? _walManager;
    private readonly bool _walAutoCheckpoint;
    private readonly bool _walEnabled;
    private readonly int _walCheckpointInterval;
    private readonly Func<Task<long>>? _checkpointFactory;
    private long _currentTimestamp = 1;
    private long _pendingCheckpointCount;
    private int _checkpointWorkerRunning;
    private long _commitCount;
    private long _nextTxnId = 1;

    public TransactionManager(
        ILockManager lockManager,
        IVersionManager versionManager,
        IObjectSerializer serializer,
        WALManager? walManager = null,
        bool walEnabled = false,
        bool walAutoCheckpoint = false,
        int walCheckpointInterval = 0,
        Func<Task<long>>? checkpointFactory = null)
    {
        _lockManager = lockManager;
        _versionManager = versionManager;
        _serializer = serializer;
        _walManager = walManager;
        _walEnabled = walEnabled;
        _walAutoCheckpoint = walAutoCheckpoint;
        _walCheckpointInterval = walCheckpointInterval;
        _checkpointFactory = checkpointFactory;
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

        TryScheduleCheckpoint();
    }

    public async Task RollbackAsync(ITransaction transaction)
    {
        // Log abort to WAL
        _walManager?.LogTransactionAbort(transaction.TransactionId);

        Exception? rollbackActionFailure = null;
        try
        {
            await transaction.RunRollbackActionsAsync();
        }
        catch (Exception ex)
        {
            rollbackActionFailure = ex;
        }

        await _versionManager.AbortAsync(transaction.TransactionId);
        await _lockManager.ReleaseAllLocksAsync(new TransactionId(transaction.TransactionId));

        if (rollbackActionFailure != null)
            throw new InvalidOperationException("Rollback completed with compensation failures.", rollbackActionFailure);
    }

    public void Dispose()
    {
        _lockManager?.Dispose();
        _versionManager?.Dispose();
    }

    private void TryScheduleCheckpoint()
    {
        if (!_walEnabled || !_walAutoCheckpoint || _walCheckpointInterval <= 0 || _checkpointFactory == null)
            return;

        var commitCount = Interlocked.Increment(ref _commitCount);
        if (commitCount % _walCheckpointInterval != 0)
            return;

        Interlocked.Increment(ref _pendingCheckpointCount);
        StartCheckpointWorkerIfNeeded();
    }

    private void StartCheckpointWorkerIfNeeded()
    {
        if (Interlocked.CompareExchange(ref _checkpointWorkerRunning, 1, 0) != 0)
            return;

        _ = ProcessCheckpointQueueAsync();
    }

    private async Task ProcessCheckpointQueueAsync()
    {
        try
        {
            while (true)
            {
                var pending = Interlocked.Exchange(ref _pendingCheckpointCount, 0);
                if (pending <= 0)
                    break;

                for (long i = 0; i < pending; i++)
                    await CreateCheckpointSafelyAsync();
            }
        }
        finally
        {
            Interlocked.Exchange(ref _checkpointWorkerRunning, 0);

            if (Volatile.Read(ref _pendingCheckpointCount) > 0)
                StartCheckpointWorkerIfNeeded();
        }
    }

    private async Task CreateCheckpointSafelyAsync()
    {
        try
        {
            await _checkpointFactory!();
        }
        catch (Exception ex)
        {
            _logger.Warning(ex, "Auto-checkpoint failed. Commit path remains successful.");
        }
    }
}
