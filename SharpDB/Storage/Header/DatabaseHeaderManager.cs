using System.Collections.Concurrent;
using System.Text.Json;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.Engine;

namespace SharpDB.Storage.Header;

public class DatabaseHeaderManager : IDatabaseHeaderManager
{
    private readonly string _headerFile;
    private readonly ConcurrentDictionary<int, Schema> _schemas = new();
    private readonly ConcurrentDictionary<int, CollectionInfo> _collections = new();
    private int _nextCollectionId = 1;
    
    public DatabaseHeaderManager(string basePath)
    {
        _headerFile = Path.Combine(basePath, "db_header.json");
        LoadHeader();
    }
    
    public Task<Schema?> GetSchemaAsync(int collectionId)
    {
        _schemas.TryGetValue(collectionId, out var schema);
        return Task.FromResult(schema);
    }
    
    public async Task SaveSchemaAsync(int collectionId, Schema schema)
    {
        schema.Validate();
        _schemas[collectionId] = schema;
        
        if (_collections.TryGetValue(collectionId, out var info))
            info.SchemaVersion = schema.Version;
        
        await SaveHeader();
    }
    
    public async Task<int> CreateCollectionAsync(string name, Schema schema)
    {
        schema.Validate();
        
        var collectionId = Interlocked.Increment(ref _nextCollectionId);
        
        _schemas[collectionId] = schema;
        _collections[collectionId] = new CollectionInfo
        {
            CollectionId = collectionId,
            Name = name,
            SchemaVersion = schema.Version,
            RecordCount = 0,
            CreatedAt = DateTime.UtcNow
        };
        
        await SaveHeader();
        return collectionId;
    }
    
    public async Task DeleteCollectionAsync(int collectionId)
    {
        _schemas.TryRemove(collectionId, out _);
        _collections.TryRemove(collectionId, out _);
        await SaveHeader();
    }
    
    public Task<List<CollectionInfo>> GetCollectionsAsync()
    {
        return Task.FromResult(_collections.Values.ToList());
    }
    
    private void LoadHeader()
    {
        if (!File.Exists(_headerFile)) return;
        
        var json = File.ReadAllText(_headerFile);
        var header = JsonSerializer.Deserialize<DatabaseHeader>(json);
        
        if (header != null)
        {
            _nextCollectionId = header.NextCollectionId;
            
            foreach (var col in header.Collections)
            {
                _collections[col.CollectionId] = new CollectionInfo
                {
                    CollectionId = col.CollectionId,
                    Name = col.Name,
                    SchemaVersion = col.Schema.Version,
                    RecordCount = col.RecordCount,
                    CreatedAt = col.CreatedAt
                };
                _schemas[col.CollectionId] = col.Schema;
            }
        }
    }
    
    private async Task SaveHeader()
    {
        var header = new DatabaseHeader
        {
            Version = 1,
            NextCollectionId = _nextCollectionId,
            Collections = _collections.Values
                .Select(c => new CollectionHeader
                {
                    CollectionId = c.CollectionId,
                    Name = c.Name,
                    Schema = _schemas[c.CollectionId],
                    RecordCount = c.RecordCount,
                    CreatedAt = c.CreatedAt
                })
                .ToList()
        };
        
        var json = JsonSerializer.Serialize(header, new JsonSerializerOptions
        {
            WriteIndented = true
        });
        
        await File.WriteAllTextAsync(_headerFile, json);
    }
}

internal class DatabaseHeader
{
    public int Version { get; set; }
    public int NextCollectionId { get; set; }
    public List<CollectionHeader> Collections { get; set; } = new();
}

internal class CollectionHeader
{
    public int CollectionId { get; set; }
    public string Name { get; set; } = "";
    public Schema Schema { get; set; } = new();
    public long RecordCount { get; set; }
    public DateTime CreatedAt { get; set; }
}