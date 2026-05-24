namespace SharpDB.V2;
using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Index;
using SharpDB.V2.Engine.Abstractions.Serialization;
using SharpDB.V2.Engine.Abstractions.Constraints;
using SharpDB.V2.Engine.Abstractions.Schema;
using SharpDB.V2.Engine.Operations;

public sealed class Collection<T, TKey>
{
    internal Collection(
        string name,
        IUniqueIndex<TKey> primaryIndex,
        IReadOnlyList<IDuplicateIndex<TKey>> secondaryIndexes,
        ISerializer<T> serializer,
        IKeyExtractor<T, TKey> keyExtractor,
        IConstraintValidator<T> constraints,
        IAutoIncrementStore autoIncrement,
        OperationContextFactory contextFactory)
    { }

    public string Name { get; } = "";
}
