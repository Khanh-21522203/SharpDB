using SharpDB.V2.Engine.Abstractions.Schema;

namespace SharpDB.V2.Engine.Schema;

public sealed class AutoIncrementStore : IAutoIncrementStore
{
    public long NextValue(string collectionName) => throw new NotImplementedException();

    public long Peek(string collectionName) => throw new NotImplementedException();

    public void Reset(string collectionName, long value) { }
}
