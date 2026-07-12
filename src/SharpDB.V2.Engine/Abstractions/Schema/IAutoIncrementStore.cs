namespace SharpDB.V2.Engine.Abstractions.Schema;

public interface IAutoIncrementStore
{
    long NextValue(string collectionName);
    long Peek(string collectionName);
    void Reset(string collectionName, long value);
}
