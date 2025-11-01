namespace SharpDB.Core.Abstractions.Index;

public interface INullableIndex<TV> : IIndex<TV>
{
    Task<List<TV>> GetNullsAsync();
    Task<bool> HasNullsAsync();
    Task<int> RemoveNullsAsync();
}