using SharpDB.V2.Engine.Abstractions.Serialization;

namespace SharpDB.V2.Engine.Serialization;

public sealed class KeyExtractor<T, TKey> : IKeyExtractor<T, TKey>
{
    private readonly Func<T, TKey> _keySelector;
    private readonly IComparer<TKey> _comparer;

    public KeyExtractor(Func<T, TKey> keySelector, IComparer<TKey>? comparer = null)
    {
        _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
        _comparer = comparer ?? ResolveDefaultComparer();
    }

    public TKey ExtractKey(T value) => _keySelector(value);

    public int CompareKeys(TKey x, TKey y) => _comparer.Compare(x, y);

    private static IComparer<TKey> ResolveDefaultComparer()
    {
        var keyType = typeof(TKey);
        if (typeof(IComparable<TKey>).IsAssignableFrom(keyType) || typeof(IComparable).IsAssignableFrom(keyType))
            return Comparer<TKey>.Default;

        throw new NotSupportedException(
            $"Type '{keyType.Name}' does not implement IComparable<{keyType.Name}> or IComparable, " +
            "so KeyExtractor cannot order keys of this type. Supply an explicit IComparer<TKey> instead.");
    }
}
