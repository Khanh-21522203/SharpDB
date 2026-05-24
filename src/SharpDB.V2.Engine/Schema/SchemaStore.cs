using SharpDB.V2.Engine.Abstractions.Schema;
using SharpDB.V2.Engine.Schema;

namespace SharpDB.V2.Engine.Schema;

public sealed class SchemaStore : ISchemaStore
{
    public CollectionSchema? TryLoad(string collectionName) => throw new NotImplementedException();

    public void Save(CollectionSchema schema) { }

    public bool Exists(string collectionName) => throw new NotImplementedException();

    public void Delete(string collectionName) { }
}
