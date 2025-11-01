namespace SharpDB.Core.Abstractions.Index;

public interface ITreeIndexManager<TK, TV> : IDisposable
    where TK : IComparable<TK>
{
    Task<TV?> GetAsync(TK key);
    Task PutAsync(TK key, TV value);
    Task<bool> RemoveAsync(TK key);
    Task FlushAsync();
}