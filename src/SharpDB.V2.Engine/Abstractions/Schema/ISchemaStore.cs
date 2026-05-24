using SharpDB.V2.Engine.Schema;

namespace SharpDB.V2.Engine.Abstractions.Schema;

public interface ISchemaStore
{
    CollectionSchema? TryLoad(string collectionName);
    void Save(CollectionSchema schema);
    bool Exists(string collectionName);
    void Delete(string collectionName);
}
