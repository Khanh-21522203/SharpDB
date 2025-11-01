namespace SharpDB.Core.Abstractions.Index;

public interface IDuplicateTreeIndexManager<TK, TV> : ITreeIndexManager<TK, TV>
    where TK : IComparable<TK>
{
    Task<List<TV>> GetAllAsync(TK key);
    Task<int> CountAsync(TK key);
}