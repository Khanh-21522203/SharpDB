namespace SharpDB.Core.Abstractions.Serialization;

/// <summary>
///     High-level object serializer for complex types.
///     Used for serializing models, entities, and composite objects.
/// </summary>
public interface IObjectSerializer
{
    /// <summary>
    ///     Serialize object to byte array.
    /// </summary>
    byte[] Serialize(object obj);

    /// <summary>
    ///     Deserialize byte array to typed object.
    /// </summary>
    T Deserialize<T>(byte[] bytes) where T : class;

    /// <summary>
    ///     Zero-copy deserialize: reads directly from a raw buffer at the given offset,
    ///     avoiding an intermediate byte[] copy. Default implementation falls back to a copy.
    /// </summary>
    T Deserialize<T>(byte[] bytes, int offset) where T : class
    {
        if (offset == 0)
            return Deserialize<T>(bytes);
        var copy = new byte[bytes.Length - offset];
        Array.Copy(bytes, offset, copy, 0, copy.Length);
        return Deserialize<T>(copy);
    }

    /// <summary>
    ///     Get serialized size (if fixed size, otherwise -1).
    /// </summary>
    int GetSize(object obj);

    /// <summary>
    ///     Check if serializer supports type.
    /// </summary>
    bool CanSerialize(Type type);
}