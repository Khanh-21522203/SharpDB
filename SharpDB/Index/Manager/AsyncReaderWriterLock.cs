namespace SharpDB.Index.Manager;

/// <summary>
/// Async-compatible reader-writer lock.
/// Multiple readers may hold the lock concurrently; writers get exclusive access.
/// Uses the "first reader acquires writer gate" pattern:
///   - First reader acquires the writer semaphore (blocks writers).
///   - Subsequent readers increment a counter without waiting on the writer gate.
///   - Last reader releases the writer semaphore (unblocks waiting writers).
///   - Writers wait exclusively on the writer semaphore and on the reader gate (to prevent new readers).
/// </summary>
internal sealed class AsyncReaderWriterLock : IDisposable
{
    // Guards increments to _readers; ensures first-reader/last-reader atomicity.
    private readonly SemaphoreSlim _readerGate = new(1, 1);
    // Held by first reader (until last reader exits) or by a writer.
    private readonly SemaphoreSlim _writerGate = new(1, 1);
    private int _readers;

    public async Task EnterReadLockAsync()
    {
        await _readerGate.WaitAsync();
        try
        {
            if (Interlocked.Increment(ref _readers) == 1)
                await _writerGate.WaitAsync(); // first reader: block writers
        }
        finally
        {
            _readerGate.Release();
        }
    }

    public void ExitReadLock()
    {
        if (Interlocked.Decrement(ref _readers) == 0)
            _writerGate.Release(); // last reader: unblock writers
    }

    public async Task EnterWriteLockAsync()
    {
        await _readerGate.WaitAsync(); // prevent new readers from entering
        await _writerGate.WaitAsync(); // wait for existing readers or writers to finish
        _readerGate.Release();         // allow readers to queue (they will wait on _writerGate)
    }

    public void ExitWriteLock() => _writerGate.Release();

    public void Dispose()
    {
        _readerGate.Dispose();
        _writerGate.Dispose();
    }
}
