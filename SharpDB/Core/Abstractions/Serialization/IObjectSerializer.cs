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
    ///     Get serialized size (if fixed size, otherwise -1).
    /// </summary>
    int GetSize(object obj);

    /// <summary>
    ///     Check if serializer supports type.
    /// </summary>
    bool CanSerialize(Type type);
}