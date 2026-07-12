using System.Text.Json;
using SharpDB.V2.Engine.Abstractions.Schema;

namespace SharpDB.V2.Engine.Schema;

/// <summary>
/// File-backed <see cref="ISchemaStore"/>: one JSON file per collection under <paramref name="directoryPath"/>.
/// Temporary until the page-based Storage engine exists, at which point this should be replaced with a
/// page-based implementation rather than kept as a fallback.
/// </summary>
public sealed class SchemaStore(string directoryPath) : ISchemaStore
{
    public CollectionSchema? TryLoad(string collectionName)
    {
        var path = PathFor(collectionName);
        return File.Exists(path)
            ? JsonSerializer.Deserialize<CollectionSchema>(File.ReadAllText(path))
            : null;
    }

    public void Save(CollectionSchema schema)
    {
        Directory.CreateDirectory(directoryPath);
        File.WriteAllText(PathFor(schema.Name), JsonSerializer.Serialize(schema));
    }

    public bool Exists(string collectionName) => File.Exists(PathFor(collectionName));

    public void Delete(string collectionName)
    {
        var path = PathFor(collectionName);
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private string PathFor(string collectionName) => Path.Combine(directoryPath, $"{collectionName}.schema.json");
}
