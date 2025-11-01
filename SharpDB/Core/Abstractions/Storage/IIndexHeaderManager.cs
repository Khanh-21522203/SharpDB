using SharpDB.DataStructures;

namespace SharpDB.Core.Abstractions.Storage;

public interface IIndexHeaderManager
{
    Task<Pointer?> GetRootPointerAsync(int indexId);
    Task SetRootPointerAsync(int indexId, Pointer pointer);
    Task<IndexMetadata?> GetMetadataAsync(int indexId);
    Task SaveMetadataAsync(int indexId, IndexMetadata metadata);
    Task DeleteIndexAsync(int indexId);
}

public class IndexMetadata
{
    public int IndexId { get; set; }
    public int Version { get; set; }
    public int Degree { get; set; }
    public long TotalNodes { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime ModifiedAt { get; set; }
}