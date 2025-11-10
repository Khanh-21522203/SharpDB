using SharpDB.DataStructures;

namespace SharpDB.WAL;

/// <summary>
/// Represents different types of WAL log records
/// </summary>
public enum LogRecordType : byte
{
    /// <summary>Transaction start</summary>
    Begin = 0x01,
    
    /// <summary>Transaction commit</summary>
    Commit = 0x02,
    
    /// <summary>Transaction abort/rollback</summary>
    Abort = 0x03,
    
    /// <summary>Page update operation</summary>
    Update = 0x04,
    
    /// <summary>Page insert operation</summary>
    Insert = 0x05,
    
    /// <summary>Page delete operation</summary>
    Delete = 0x06,
    
    /// <summary>Checkpoint start</summary>
    CheckpointStart = 0x07,
    
    /// <summary>Checkpoint end</summary>
    CheckpointEnd = 0x08,
    
    /// <summary>Compensation log record (for undo)</summary>
    CLR = 0x09
}

/// <summary>
/// Base class for all WAL log records
/// </summary>
public abstract class LogRecord
{
    public long LSN { get; set; }  // Log Sequence Number
    public long TransactionId { get; set; }
    public LogRecordType Type { get; set; }  // Changed from protected set to public set for CLR handling
    public long PrevLSN { get; set; }  // Previous LSN for this transaction
    public DateTime Timestamp { get; set; }
    
    protected LogRecord(LogRecordType type, long transactionId)
    {
        Type = type;
        TransactionId = transactionId;
        Timestamp = DateTime.UtcNow;
    }
    
    public abstract byte[] Serialize();
    public abstract void Deserialize(byte[] data);
    public abstract int GetSize();
}

/// <summary>
/// Transaction begin log record
/// </summary>
public class BeginLogRecord : LogRecord
{
    public BeginLogRecord(long transactionId) 
        : base(LogRecordType.Begin, transactionId)
    {
    }
    
    public override byte[] Serialize()
    {
        var size = GetSize();
        var buffer = new byte[size];
        var offset = 0;
        
        // Write base fields
        buffer[offset++] = (byte)Type;
        BitConverter.GetBytes(LSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(TransactionId).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(PrevLSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(Timestamp.Ticks).CopyTo(buffer, offset);
        
        return buffer;
    }
    
    public override void Deserialize(byte[] data)
    {
        var offset = 1; // Skip type byte
        LSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        TransactionId = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        PrevLSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        Timestamp = new DateTime(BitConverter.ToInt64(data, offset));
    }
    
    public override int GetSize()
    {
        return 1 + sizeof(long) * 4; // Type + LSN + TxnId + PrevLSN + Timestamp
    }
}

/// <summary>
/// Transaction commit log record
/// </summary>
public class CommitLogRecord : LogRecord
{
    public CommitLogRecord(long transactionId) 
        : base(LogRecordType.Commit, transactionId)
    {
    }
    
    public override byte[] Serialize()
    {
        var size = GetSize();
        var buffer = new byte[size];
        var offset = 0;
        
        buffer[offset++] = (byte)Type;
        BitConverter.GetBytes(LSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(TransactionId).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(PrevLSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(Timestamp.Ticks).CopyTo(buffer, offset);
        
        return buffer;
    }
    
    public override void Deserialize(byte[] data)
    {
        var offset = 1;
        LSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        TransactionId = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        PrevLSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        Timestamp = new DateTime(BitConverter.ToInt64(data, offset));
    }
    
    public override int GetSize()
    {
        return 1 + sizeof(long) * 4;
    }
}

/// <summary>
/// Transaction abort log record
/// </summary>
public class AbortLogRecord : LogRecord
{
    public AbortLogRecord(long transactionId) 
        : base(LogRecordType.Abort, transactionId)
    {
    }
    
    public override byte[] Serialize()
    {
        var size = GetSize();
        var buffer = new byte[size];
        var offset = 0;
        
        buffer[offset++] = (byte)Type;
        BitConverter.GetBytes(LSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(TransactionId).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(PrevLSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(Timestamp.Ticks).CopyTo(buffer, offset);
        
        return buffer;
    }
    
    public override void Deserialize(byte[] data)
    {
        var offset = 1;
        LSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        TransactionId = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        PrevLSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        Timestamp = new DateTime(BitConverter.ToInt64(data, offset));
    }
    
    public override int GetSize()
    {
        return 1 + sizeof(long) * 4;
    }
}

/// <summary>
/// Data update log record
/// </summary>
public class UpdateLogRecord : LogRecord
{
    public int CollectionId { get; set; }
    public Pointer PagePointer { get; set; }
    public byte[] BeforeImage { get; set; } = Array.Empty<byte>();  // Data before update
    public byte[] AfterImage { get; set; } = Array.Empty<byte>();   // Data after update
    public long UndoNextLSN { get; set; }  // For compensation log records
    
    public UpdateLogRecord(long transactionId, int collectionId, Pointer pagePointer) 
        : base(LogRecordType.Update, transactionId)
    {
        CollectionId = collectionId;
        PagePointer = pagePointer;
    }
    
    public override byte[] Serialize()
    {
        var size = GetSize();
        var buffer = new byte[size];
        var offset = 0;
        
        // Write base fields
        buffer[offset++] = (byte)Type;
        BitConverter.GetBytes(LSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(TransactionId).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(PrevLSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(Timestamp.Ticks).CopyTo(buffer, offset);
        offset += sizeof(long);
        
        // Write update-specific fields
        BitConverter.GetBytes(CollectionId).CopyTo(buffer, offset);
        offset += sizeof(int);
        PagePointer.FillBytes(buffer, offset);
        offset += Pointer.ByteSize;
        BitConverter.GetBytes(UndoNextLSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        
        // Write before image
        BitConverter.GetBytes(BeforeImage.Length).CopyTo(buffer, offset);
        offset += sizeof(int);
        BeforeImage.CopyTo(buffer, offset);
        offset += BeforeImage.Length;
        
        // Write after image
        BitConverter.GetBytes(AfterImage.Length).CopyTo(buffer, offset);
        offset += sizeof(int);
        AfterImage.CopyTo(buffer, offset);
        
        return buffer;
    }
    
    public override void Deserialize(byte[] data)
    {
        var offset = 1;
        LSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        TransactionId = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        PrevLSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        Timestamp = new DateTime(BitConverter.ToInt64(data, offset));
        offset += sizeof(long);
        
        CollectionId = BitConverter.ToInt32(data, offset);
        offset += sizeof(int);
        PagePointer = Pointer.FromBytes(data, offset);
        offset += Pointer.ByteSize;
        UndoNextLSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        
        var beforeLen = BitConverter.ToInt32(data, offset);
        offset += sizeof(int);
        BeforeImage = new byte[beforeLen];
        Array.Copy(data, offset, BeforeImage, 0, beforeLen);
        offset += beforeLen;
        
        var afterLen = BitConverter.ToInt32(data, offset);
        offset += sizeof(int);
        AfterImage = new byte[afterLen];
        Array.Copy(data, offset, AfterImage, 0, afterLen);
    }
    
    public override int GetSize()
    {
        return 1 + sizeof(long) * 5 + sizeof(int) + Pointer.ByteSize + 
               sizeof(int) * 2 + BeforeImage.Length + AfterImage.Length;
    }
}

/// <summary>
/// Checkpoint log record
/// </summary>
public class CheckpointLogRecord : LogRecord
{
    public bool IsStart { get; set; }
    public List<long> ActiveTransactions { get; set; } = new();
    public Dictionary<long, long> TransactionLastLSN { get; set; } = new();
    
    public CheckpointLogRecord(bool isStart) 
        : base(isStart ? LogRecordType.CheckpointStart : LogRecordType.CheckpointEnd, 0)
    {
        IsStart = isStart;
    }
    
    public override byte[] Serialize()
    {
        var size = GetSize();
        var buffer = new byte[size];
        var offset = 0;
        
        buffer[offset++] = (byte)Type;
        BitConverter.GetBytes(LSN).CopyTo(buffer, offset);
        offset += sizeof(long);
        BitConverter.GetBytes(Timestamp.Ticks).CopyTo(buffer, offset);
        offset += sizeof(long);
        
        // Write active transactions
        BitConverter.GetBytes(ActiveTransactions.Count).CopyTo(buffer, offset);
        offset += sizeof(int);
        foreach (var txnId in ActiveTransactions)
        {
            BitConverter.GetBytes(txnId).CopyTo(buffer, offset);
            offset += sizeof(long);
        }
        
        // Write transaction LSN mapping
        BitConverter.GetBytes(TransactionLastLSN.Count).CopyTo(buffer, offset);
        offset += sizeof(int);
        foreach (var kvp in TransactionLastLSN)
        {
            BitConverter.GetBytes(kvp.Key).CopyTo(buffer, offset);
            offset += sizeof(long);
            BitConverter.GetBytes(kvp.Value).CopyTo(buffer, offset);
            offset += sizeof(long);
        }
        
        return buffer;
    }
    
    public override void Deserialize(byte[] data)
    {
        var offset = 1;
        LSN = BitConverter.ToInt64(data, offset);
        offset += sizeof(long);
        Timestamp = new DateTime(BitConverter.ToInt64(data, offset));
        offset += sizeof(long);
        
        var activeCount = BitConverter.ToInt32(data, offset);
        offset += sizeof(int);
        ActiveTransactions = new List<long>(activeCount);
        for (int i = 0; i < activeCount; i++)
        {
            ActiveTransactions.Add(BitConverter.ToInt64(data, offset));
            offset += sizeof(long);
        }
        
        var lsnCount = BitConverter.ToInt32(data, offset);
        offset += sizeof(int);
        TransactionLastLSN = new Dictionary<long, long>(lsnCount);
        for (int i = 0; i < lsnCount; i++)
        {
            var txnId = BitConverter.ToInt64(data, offset);
            offset += sizeof(long);
            var lsn = BitConverter.ToInt64(data, offset);
            offset += sizeof(long);
            TransactionLastLSN[txnId] = lsn;
        }
    }
    
    public override int GetSize()
    {
        return 1 + sizeof(long) * 2 + sizeof(int) * 2 + 
               ActiveTransactions.Count * sizeof(long) +
               TransactionLastLSN.Count * sizeof(long) * 2;
    }
}
