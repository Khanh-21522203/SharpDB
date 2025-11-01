namespace SharpDB.Core.Abstractions.Index;

public interface IIndex<TV>
{
    Task<TV?> GetAsync(object key);
    Task PutAsync(object key, TV value);
    Task<bool> RemoveAsync(object key);
}