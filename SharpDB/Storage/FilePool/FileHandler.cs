namespace SharpDB.Storage.FilePool;

public class FileHandler(string filePath) : IDisposable
{
    public string FilePath { get; } = filePath;
    public FileStream FileStream { get; private set; } = new(
        filePath,
        FileMode.OpenOrCreate,
        FileAccess.ReadWrite,
        FileShare.None,
        bufferSize: 4096,
        useAsync: true
    );

    private int _usageCount;
    private readonly Lock _lock = new();
    
    public int UsageCount
    {
        get { lock (_lock) return _usageCount; }
    }

    public void IncrementUsage()
    {
        lock (_lock)
        {
            _usageCount++;
        }
    }
    
    public void DecrementUsage()
    {
        lock (_lock)
        {
            _usageCount--;
        }
    }
    
    public void Dispose()
    {
        FileStream?.Dispose();
    }
}