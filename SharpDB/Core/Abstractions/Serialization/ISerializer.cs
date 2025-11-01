namespace SharpDB.Core.Abstractions.Serialization;

public interface ISerializer<T>
{
    byte[] Serialize(T obj);
    T Deserialize(byte[] bytes, int offset = 0);
    int Size { get; }
}