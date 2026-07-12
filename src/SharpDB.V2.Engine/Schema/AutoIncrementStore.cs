using System.Collections.Concurrent;
using SharpDB.V2.Engine.Abstractions.Schema;

namespace SharpDB.V2.Engine.Schema;

public sealed class AutoIncrementStore : IAutoIncrementStore
{
    private readonly ConcurrentDictionary<string, long> _counters = new();

    public long NextValue(string collectionName) =>
        _counters.AddOrUpdate(collectionName, 1, (_, current) => current + 1);

    public long Peek(string collectionName) =>
        _counters.GetValueOrDefault(collectionName, 0);

    public void Reset(string collectionName, long value) =>
        _counters[collectionName] = value;
}
