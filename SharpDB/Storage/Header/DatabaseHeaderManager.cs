using System.Collections.Concurrent;
using System.Text.Json;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.Engine;

namespace SharpDB.Storage.Header;

public class DatabaseHeaderManager : IDatabaseHeaderManager
{
    private readonly ConcurrentDictionary<int, CollectionInfo> _collections = new();
    private readonly string _headerFile;
    private readonly ConcurrentDictionary<int, Schema> _schemas = new();
    private int _nextCollectionId = 1;

    // Block-based sequence allocation: each flush reserves a block of IDs on disk so that
    // normal inserts never touch disk.  On crash we skip at most (BlockSize-1) IDs but never repeat.
    private const int SequenceBlockSize = 100;
    private readonly Dictionary<(int CollectionId, string Field), long> _inMemoryCounters = new();
    private readonly SemaphoreSlim _sequenceLock = new(1, 1);

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

    public async Task<long> GetNextSequenceValueAsync(int collectionId, string fieldName)
    {
        if (!_collections.TryGetValue(collectionId, out var info))
            throw new InvalidOperationException($"Collection {collectionId} not found");

        var key = (collectionId, fieldName);

        await _sequenceLock.WaitAsync();
        long next;
        bool needsFlush;
        try
        {
            if (!_inMemoryCounters.TryGetValue(key, out var current))
            {
                // First use: initialize from the disk-persisted reservation.
                info.SequenceCounters.TryGetValue(fieldName, out current);
            }

            next = current + 1;
            _inMemoryCounters[key] = next;

            // Flush only when the pre-allocated block is exhausted.
            info.SequenceCounters.TryGetValue(fieldName, out var diskReserved);
            needsFlush = next > diskReserved;
            if (needsFlush)
                info.SequenceCounters[fieldName] = next + SequenceBlockSize - 1;
        }
        finally
        {
            _sequenceLock.Release();
        }

        if (needsFlush)
            await SaveHeader();

        return next;
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
                    CreatedAt = col.CreatedAt,
                    SequenceCounters = col.SequenceCounters ?? new()
                };
                _schemas[col.CollectionId] = col.Schema;

                // Seed in-memory counters from the persisted block-end reservation.
                // On restart we begin from the block end so old IDs are never reused.
                if (col.SequenceCounters != null)
                    foreach (var kvp in col.SequenceCounters)
                        _inMemoryCounters[(col.CollectionId, kvp.Key)] = kvp.Value;
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
                    CreatedAt = c.CreatedAt,
                    SequenceCounters = c.SequenceCounters
                })
                .ToList()
        };

        var json = JsonSerializer.Serialize(header, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        var tempFile = $"{_headerFile}.tmp";
        await File.WriteAllTextAsync(tempFile, json);

        await using (var fs = new FileStream(
                         tempFile,
                         FileMode.Open,
                         FileAccess.ReadWrite,
                         FileShare.None,
                         4096,
                         FileOptions.WriteThrough))
        {
            await fs.FlushAsync();
        }

        File.Move(tempFile, _headerFile, true);
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
    public Dictionary<string, long> SequenceCounters { get; set; } = new();
}
