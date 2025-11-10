using System.Collections.Concurrent;
using Serilog;
using SharpDB.DataStructures;

namespace SharpDB.WAL;

/// <summary>
/// Write-Ahead Logging Manager
/// Manages log writing, reading, and recovery operations
/// </summary>
public class WALManager : IDisposable
{
    private readonly string _logDirectory;
    private readonly ILogger _logger = Log.ForContext<WALManager>();
    private readonly object _writeLock = new();
    private readonly ConcurrentDictionary<long, long> _transactionLastLSN = new();
    
    private FileStream? _currentLogFile;
    private BinaryWriter? _logWriter;
    private long _nextLSN = 1;
    private long _lastCheckpointLSN = 0;
    private readonly int _maxLogFileSize;
    private int _currentLogFileNumber = 0;
    private bool _disposed = false;
    
    // Buffer for group commit
    private readonly List<LogRecord> _pendingRecords = new();
    private readonly Timer _flushTimer;
    private readonly int _flushIntervalMs = 100; // Flush every 100ms
    
    public WALManager(string basePath, int maxLogFileSize = 10 * 1024 * 1024) // 10MB default
    {
        _logDirectory = Path.Combine(basePath, "wal");
        _maxLogFileSize = maxLogFileSize;
        
        Directory.CreateDirectory(_logDirectory);
        
        // Find the latest log file
        var logFiles = Directory.GetFiles(_logDirectory, "wal_*.log")
            .OrderByDescending(f => f)
            .ToList();
            
        if (logFiles.Any())
        {
            var lastFile = Path.GetFileNameWithoutExtension(logFiles.First());
            _currentLogFileNumber = int.Parse(lastFile.Split('_')[1]);
            
            // Read last LSN from the file
            _nextLSN = GetLastLSNFromFile(logFiles.First()) + 1;
        }
        
        OpenNewLogFile();
        
        // Setup flush timer for group commit
        _flushTimer = new Timer(_ => FlushPendingRecords(), null, _flushIntervalMs, _flushIntervalMs);
        
        _logger.Information("WAL Manager initialized with log directory: {LogDirectory}", _logDirectory);
    }
    
    /// <summary>
    /// Write a log record to WAL
    /// </summary>
    public long WriteLogRecord(LogRecord record)
    {
        lock (_writeLock)
        {
            // Assign LSN
            record.LSN = _nextLSN++;
            
            // Update transaction's last LSN
            if (record.TransactionId > 0)
            {
                if (_transactionLastLSN.TryGetValue(record.TransactionId, out var lastLSN))
                {
                    record.PrevLSN = lastLSN;
                }
                _transactionLastLSN[record.TransactionId] = record.LSN;
            }
            
            // Add to pending records for group commit
            _pendingRecords.Add(record);
            
            // If this is a commit record, flush immediately
            if (record.Type == LogRecordType.Commit)
            {
                FlushPendingRecords();
            }
            
            // Check if we need to switch to a new log file
            if (_currentLogFile != null && _currentLogFile.Length > _maxLogFileSize)
            {
                SwitchToNewLogFile();
            }
            
            return record.LSN;
        }
    }
    
    /// <summary>
    /// Force flush all pending records to disk
    /// </summary>
    public void Flush()
    {
        lock (_writeLock)
        {
            FlushPendingRecords();
        }
    }
    
    /// <summary>
    /// Write a begin transaction record
    /// </summary>
    public long LogTransactionBegin(long transactionId)
    {
        var record = new BeginLogRecord(transactionId);
        return WriteLogRecord(record);
    }
    
    /// <summary>
    /// Write a commit transaction record
    /// </summary>
    public long LogTransactionCommit(long transactionId)
    {
        var record = new CommitLogRecord(transactionId);
        var lsn = WriteLogRecord(record);
        
        // Remove transaction from active list
        _transactionLastLSN.TryRemove(transactionId, out _);
        
        return lsn;
    }
    
    /// <summary>
    /// Write an abort transaction record
    /// </summary>
    public long LogTransactionAbort(long transactionId)
    {
        var record = new AbortLogRecord(transactionId);
        var lsn = WriteLogRecord(record);
        
        // Remove transaction from active list
        _transactionLastLSN.TryRemove(transactionId, out _);
        
        return lsn;
    }
    
    /// <summary>
    /// Log a data update operation
    /// </summary>
    public long LogUpdate(long transactionId, int collectionId, Pointer pagePointer, 
        byte[] beforeImage, byte[] afterImage)
    {
        var record = new UpdateLogRecord(transactionId, collectionId, pagePointer)
        {
            BeforeImage = beforeImage,
            AfterImage = afterImage
        };
        
        return WriteLogRecord(record);
    }
    
    /// <summary>
    /// Create a checkpoint
    /// </summary>
    public async Task<long> CreateCheckpointAsync()
    {
        _logger.Information("Creating checkpoint");
        
        // Write checkpoint start record
        var startRecord = new CheckpointLogRecord(true)
        {
            ActiveTransactions = _transactionLastLSN.Keys.ToList(),
            TransactionLastLSN = new Dictionary<long, long>(_transactionLastLSN)
        };
        
        var startLSN = WriteLogRecord(startRecord);
        _lastCheckpointLSN = startLSN;
        
        // Flush all pending records
        Flush();
        
        // Write checkpoint end record
        var endRecord = new CheckpointLogRecord(false);
        var endLSN = WriteLogRecord(endRecord);
        
        Flush();
        
        _logger.Information("Checkpoint created at LSN {StartLSN} - {EndLSN}", startLSN, endLSN);
        
        return startLSN;
    }
    
    /// <summary>
    /// Perform recovery from WAL
    /// </summary>
    public async Task<RecoveryResult> RecoverAsync()
    {
        _logger.Information("Starting recovery from WAL");
        
        var result = new RecoveryResult();
        var logFiles = Directory.GetFiles(_logDirectory, "wal_*.log")
            .OrderBy(f => f)
            .ToList();
        
        if (!logFiles.Any())
        {
            _logger.Information("No log files found, nothing to recover");
            return result;
        }
        
        // Phase 1: Analysis - Find winners and losers
        var committedTransactions = new HashSet<long>();
        var abortedTransactions = new HashSet<long>();
        var activeTransactions = new Dictionary<long, List<LogRecord>>();
        
        foreach (var logFile in logFiles)
        {
            await AnalyzeLogFile(logFile, committedTransactions, abortedTransactions, activeTransactions);
        }
        
        result.CommittedTransactions = committedTransactions.Count;
        result.AbortedTransactions = abortedTransactions.Count;
        result.UnfinishedTransactions = activeTransactions.Count;
        
        // Phase 2: Redo - Apply all committed transactions
        _logger.Information("Starting REDO phase");
        foreach (var logFile in logFiles)
        {
            await RedoLogFile(logFile, committedTransactions);
        }
        
        // Phase 3: Undo - Rollback uncommitted transactions
        _logger.Information("Starting UNDO phase");
        foreach (var (txnId, records) in activeTransactions)
        {
            if (!committedTransactions.Contains(txnId))
            {
                await UndoTransaction(txnId, records);
            }
        }
        
        _logger.Information("Recovery completed. Committed: {Committed}, Aborted: {Aborted}, Rolled back: {RolledBack}",
            result.CommittedTransactions, result.AbortedTransactions, result.UnfinishedTransactions);
        
        return result;
    }
    
    private async Task AnalyzeLogFile(string logFile, HashSet<long> committed, 
        HashSet<long> aborted, Dictionary<long, List<LogRecord>> active)
    {
        using var stream = new FileStream(logFile, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(stream);
        
        while (stream.Position < stream.Length)
        {
            try
            {
                var record = ReadLogRecord(reader);
                
                if (record == null) break;
                
                switch (record.Type)
                {
                    case LogRecordType.Begin:
                        if (!active.ContainsKey(record.TransactionId))
                            active[record.TransactionId] = new List<LogRecord>();
                        active[record.TransactionId].Add(record);
                        break;
                        
                    case LogRecordType.Commit:
                        committed.Add(record.TransactionId);
                        if (active.ContainsKey(record.TransactionId))
                            active.Remove(record.TransactionId);
                        break;
                        
                    case LogRecordType.Abort:
                        aborted.Add(record.TransactionId);
                        if (active.ContainsKey(record.TransactionId))
                            active.Remove(record.TransactionId);
                        break;
                        
                    case LogRecordType.Update:
                    case LogRecordType.Insert:
                    case LogRecordType.Delete:
                        if (active.ContainsKey(record.TransactionId))
                            active[record.TransactionId].Add(record);
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "Error reading log record, may be corrupted");
                break;
            }
        }
    }
    
    private async Task RedoLogFile(string logFile, HashSet<long> committedTransactions)
    {
        using var stream = new FileStream(logFile, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(stream);
        
        while (stream.Position < stream.Length)
        {
            try
            {
                var record = ReadLogRecord(reader);
                if (record == null) break;
                
                // Only redo operations from committed transactions
                if (committedTransactions.Contains(record.TransactionId))
                {
                    if (record is UpdateLogRecord updateRecord)
                    {
                        // Apply the after image
                        await ApplyUpdate(updateRecord);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "Error during REDO phase");
                break;
            }
        }
    }
    
    private async Task UndoTransaction(long transactionId, List<LogRecord> records)
    {
        // Process records in reverse order
        for (int i = records.Count - 1; i >= 0; i--)
        {
            if (records[i] is UpdateLogRecord updateRecord)
            {
                // Apply the before image to undo the operation
                await ApplyUndo(updateRecord);
                
                // Write a compensation log record (CLR)
                var clr = new UpdateLogRecord(transactionId, updateRecord.CollectionId, updateRecord.PagePointer)
                {
                    BeforeImage = updateRecord.AfterImage,
                    AfterImage = updateRecord.BeforeImage,
                    UndoNextLSN = updateRecord.PrevLSN
                };
                clr.Type = LogRecordType.CLR;
                WriteLogRecord(clr);
            }
        }
        
        // Write abort record
        LogTransactionAbort(transactionId);
    }
    
    private async Task ApplyUpdate(UpdateLogRecord record)
    {
        // TODO: Apply the update to the actual page
        // This would interact with PageManager to apply the after image
        _logger.Debug("Applying update for collection {CollectionId} at pointer {Pointer}", 
            record.CollectionId, record.PagePointer);
    }
    
    private async Task ApplyUndo(UpdateLogRecord record)
    {
        // TODO: Apply the undo to the actual page
        // This would interact with PageManager to apply the before image
        _logger.Debug("Applying undo for collection {CollectionId} at pointer {Pointer}", 
            record.CollectionId, record.PagePointer);
    }
    
    private LogRecord? ReadLogRecord(BinaryReader reader)
    {
        if (reader.BaseStream.Position >= reader.BaseStream.Length)
            return null;
            
        var typeByte = reader.ReadByte();
        var type = (LogRecordType)typeByte;
        
        // Read the rest based on type
        LogRecord record = type switch
        {
            LogRecordType.Begin => new BeginLogRecord(0),
            LogRecordType.Commit => new CommitLogRecord(0),
            LogRecordType.Abort => new AbortLogRecord(0),
            LogRecordType.Update => new UpdateLogRecord(0, 0, Pointer.Empty()),
            LogRecordType.CheckpointStart => new CheckpointLogRecord(true),
            LogRecordType.CheckpointEnd => new CheckpointLogRecord(false),
            _ => throw new InvalidOperationException($"Unknown log record type: {type}")
        };
        
        // Read size first to know how much to read
        var size = reader.ReadInt32();
        var data = new byte[size];
        data[0] = typeByte;
        reader.Read(data, 1, size - 1);
        
        record.Deserialize(data);
        return record;
    }
    
    private void FlushPendingRecords()
    {
        if (_pendingRecords.Count == 0 || _logWriter == null)
            return;
            
        foreach (var record in _pendingRecords)
        {
            var data = record.Serialize();
            _logWriter.Write(data.Length);
            _logWriter.Write(data);
        }
        
        _logWriter.Flush();
        _currentLogFile?.Flush();
        
        _pendingRecords.Clear();
    }
    
    private void OpenNewLogFile()
    {
        var fileName = $"wal_{_currentLogFileNumber:D8}.log";
        var filePath = Path.Combine(_logDirectory, fileName);
        
        _currentLogFile = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
        _logWriter = new BinaryWriter(_currentLogFile);
        
        _logger.Information("Opened new log file: {FileName}", fileName);
    }
    
    private void SwitchToNewLogFile()
    {
        FlushPendingRecords();
        
        _logWriter?.Close();
        _currentLogFile?.Close();
        
        _currentLogFileNumber++;
        OpenNewLogFile();
    }
    
    private long GetLastLSNFromFile(string logFile)
    {
        long lastLSN = 0;
        
        try
        {
            using var stream = new FileStream(logFile, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(stream);
            
            while (stream.Position < stream.Length)
            {
                var record = ReadLogRecord(reader);
                if (record != null && record.LSN > lastLSN)
                {
                    lastLSN = record.LSN;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Warning(ex, "Error reading last LSN from file {File}", logFile);
        }
        
        return lastLSN;
    }
    
    public void Dispose()
    {
        if (_disposed) return;
        
        lock (_writeLock)
        {
            FlushPendingRecords();
            _flushTimer?.Dispose();
            _logWriter?.Dispose();
            _currentLogFile?.Dispose();
            _disposed = true;
        }
        
        _logger.Information("WAL Manager disposed");
    }
}

/// <summary>
/// Result of recovery operation
/// </summary>
public class RecoveryResult
{
    public int CommittedTransactions { get; set; }
    public int AbortedTransactions { get; set; }
    public int UnfinishedTransactions { get; set; }
    public DateTime RecoveryTime { get; set; } = DateTime.UtcNow;
}
