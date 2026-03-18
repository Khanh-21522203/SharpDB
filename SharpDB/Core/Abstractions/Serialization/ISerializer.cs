namespace SharpDB.Core.Abstractions.Serialization;

public interface ISerializer<T>
{
    int Size { get; }
    byte[] Serialize(T obj);

    /// <summary>
    /// Writes the serialized form of <paramref name="obj"/> directly into <paramref name="dest"/>,
    /// avoiding intermediate byte[] allocation. <paramref name="dest"/> must be at least Size bytes.
    /// </summary>
    void SerializeTo(T obj, Span<byte> dest);

    T Deserialize(byte[] bytes, int offset = 0);
}