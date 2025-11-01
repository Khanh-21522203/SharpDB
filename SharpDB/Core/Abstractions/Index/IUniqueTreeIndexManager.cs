namespace SharpDB.Core.Abstractions.Index;

public interface IUniqueTreeIndexManager<TK, TV> : ITreeIndexManager<TK, TV>
    where TK : IComparable<TK>
{
    Task<bool> ContainsKeyAsync(TK key);
    Task<int> CountAsync();
}