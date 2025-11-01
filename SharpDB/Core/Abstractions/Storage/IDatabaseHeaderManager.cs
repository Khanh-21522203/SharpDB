using SharpDB.Engine;

namespace SharpDB.Core.Abstractions.Storage;

public interface IDatabaseHeaderManager
{
    Task<Schema?> GetSchemaAsync(int collectionId);
    Task SaveSchemaAsync(int collectionId, Schema schema);
    Task<int> CreateCollectionAsync(string name, Schema schema);
    Task DeleteCollectionAsync(int collectionId);
    Task<List<CollectionInfo>> GetCollectionsAsync();
}

public class CollectionInfo
{
    public int CollectionId { get; set; }
    public string Name { get; set; } = "";
    public int SchemaVersion { get; set; }
    public long RecordCount { get; set; }
    public DateTime CreatedAt { get; set; }
}