using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Core.Abstractions.Storage;

public enum ReadVisibility
{
    Session,
    Committed
}

public enum ScanVisibility
{
    Session,
    Committed
}

public enum Durability
{
    Buffered,
    Immediate
}

public abstract record DataMutation(int CollectionId)
{
    public sealed record Insert(int CollectionId, int SchemeId, int Version, byte[] Data, Durability Durability)
        : DataMutation(CollectionId);

    public sealed record Update(Pointer Pointer, byte[] Data, Durability Durability)
        : DataMutation(Pointer.Chunk);

    public sealed record Delete(Pointer Pointer, Durability Durability)
        : DataMutation(Pointer.Chunk);

    public sealed record Commit(int CollectionId = -1)
        : DataMutation(CollectionId);
}

public interface IPageDataWorkspace
{
    Task<Pointer> ApplyAsync(DataMutation mutation, CancellationToken ct = default);
    Task<DBObject?> ReadAsync(Pointer pointer, ReadVisibility visibility = ReadVisibility.Session, CancellationToken ct = default);
    IAsyncEnumerable<DBObject> ScanAsync(int collectionId, ScanVisibility visibility = ScanVisibility.Committed, CancellationToken ct = default);
}
