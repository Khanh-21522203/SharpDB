using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Index;

public interface IQueryable<TK, TV> where TK : IComparable<TK>
{
    IAsyncEnumerable<KeyValue<TK, TV>> GreaterThanAsync(TK key);
    IAsyncEnumerable<KeyValue<TK, TV>> LessThanAsync(TK key);
    IAsyncEnumerable<KeyValue<TK, TV>> RangeAsync(TK minKey, TK maxKey);
}