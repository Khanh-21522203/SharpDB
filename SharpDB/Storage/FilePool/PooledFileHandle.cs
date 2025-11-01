namespace SharpDB.Storage.FilePool;

/// <summary>
/// Pooled file handle wrapper with automatic return to pool.
/// Implements IDisposable for using pattern.
/// </summary>
public class PooledFileHandle(FileStream handle, Action returnToPool) : IDisposable
{
    private readonly FileStream _handle = handle ?? throw new ArgumentNullException(nameof(handle));
    private readonly Action _returnToPool = returnToPool ?? throw new ArgumentNullException(nameof(returnToPool));
    private bool _disposed;
    
    public FileStream Handle => _handle;

    public void Dispose()
    {
        if (_disposed)
            return;
        
        _disposed = true;
        _returnToPool();
    }
}