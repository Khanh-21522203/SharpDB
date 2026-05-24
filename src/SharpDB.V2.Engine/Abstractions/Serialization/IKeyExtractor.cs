namespace SharpDB.V2.Engine.Abstractions.Serialization;

public interface IKeyExtractor<T, TKey>
{
    TKey ExtractKey(T value);
    int CompareKeys(TKey x, TKey y);
}
