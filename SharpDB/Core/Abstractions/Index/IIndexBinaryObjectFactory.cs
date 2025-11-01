namespace SharpDB.Core.Abstractions.Index;

/// <summary>
///     Factory for creating IIndexBinaryObject instances.
///     Used by B+ Tree to serialize/deserialize objects.
/// </summary>
public interface IIndexBinaryObjectFactory<T>
{
    /// <summary>
    ///     Fixed size for all objects of this type.
    /// </summary>
    int Size { get; }

    /// <summary>
    ///     Runtime type information.
    /// </summary>
    Type Type { get; }

    /// <summary>
    ///     Create from object (serialize).
    /// </summary>
    IIndexBinaryObject<T> Create(T obj);

    /// <summary>
    ///     Create from bytes (deserialize).
    /// </summary>
    IIndexBinaryObject<T> Create(byte[] bytes, int offset);

    /// <summary>
    ///     Create empty/default instance.
    /// </summary>
    IIndexBinaryObject<T> CreateEmpty();
}