namespace SharpDB.Core.Abstractions.Serialization;

public interface ISerializer<T>
{
    int Size { get; }
    byte[] Serialize(T obj);
    T Deserialize(byte[] bytes, int offset = 0);
}