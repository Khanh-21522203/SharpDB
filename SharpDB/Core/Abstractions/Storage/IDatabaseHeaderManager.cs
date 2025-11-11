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