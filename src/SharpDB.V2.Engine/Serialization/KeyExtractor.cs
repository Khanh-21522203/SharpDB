using SharpDB.V2.Engine.Abstractions.Serialization;

namespace SharpDB.V2.Engine.Serialization;

public sealed class KeyExtractor<T, TKey> : IKeyExtractor<T, TKey>
{
    public TKey ExtractKey(T value) => throw new NotImplementedException();

    public int CompareKeys(TKey x, TKey y) => throw new NotImplementedException();
}
