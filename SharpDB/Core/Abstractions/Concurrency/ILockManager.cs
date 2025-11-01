namespace SharpDB.Core.Abstractions.Concurrency;

public interface ILockManager : IDisposable
{
    Task<bool> AcquireLockAsync(ResourceId resourceId, TransactionId txnId, LockMode mode, TimeSpan timeout);
    Task ReleaseLockAsync(ResourceId resourceId, TransactionId txnId);
    Task ReleaseAllLocksAsync(TransactionId txnId);
    Task<bool> IsLockedAsync(ResourceId resourceId);
}

public enum LockMode
{
    Shared,
    Exclusive
}

public record ResourceId(string Type, object Id);
public record TransactionId(long Id);