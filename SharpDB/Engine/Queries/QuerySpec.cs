namespace SharpDB.Engine.Queries;

public abstract record QuerySpec<TKey>
    where TKey : IComparable<TKey>
{
    public sealed record Range(TKey MinInclusive, TKey MaxInclusive) : QuerySpec<TKey>;
    public sealed record GreaterThan(TKey Key) : QuerySpec<TKey>;
    public sealed record LessThan(TKey Key) : QuerySpec<TKey>;
}
