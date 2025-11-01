using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

/// <summary>
///     Base class for IndexBinaryObject implementations.
///     Handles common byte array management.
/// </summary>
public abstract class AbstractIndexBinaryObject<T> : IIndexBinaryObject<T>
{
    protected byte[] _bytes;

    protected AbstractIndexBinaryObject(byte[] bytes)
    {
        _bytes = bytes;
    }

    protected AbstractIndexBinaryObject()
    {
        _bytes = new byte[Size];
    }

    // Must be implemented by derived classes
    public abstract T AsObject();
    public abstract int Size { get; }

    // Provided by base class
    public byte[] GetBytes()
    {
        return _bytes;
    }

    public override string ToString()
    {
        return $"{GetType().Name}[{AsObject()}]";
    }
}