namespace SharpDB.V2;

public sealed class CollectionOptions<T, TKey>
{
    public Func<T, TKey>? KeySelector { get; init; }
    public bool AutoIncrement { get; init; }
}
