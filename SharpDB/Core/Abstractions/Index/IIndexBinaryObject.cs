namespace SharpDB.Core.Abstractions.Index;

/// <summary>
/// Represents a binary-serializable object for B+ Tree storage.
/// Instances wrap a single value and provide byte representation.
/// </summary>
public interface IIndexBinaryObject<out T>
{
    /// <summary>
    /// Get the wrapped object.
    /// </summary>
    T AsObject();
    
    /// <summary>
    /// Get byte array representation.
    /// </summary>
    byte[] GetBytes();
    
    /// <summary>
    /// Fixed size in bytes (must be constant for all instances of same type).
    /// </summary>
    int Size { get; }
}