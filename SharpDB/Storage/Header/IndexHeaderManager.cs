using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Header;

public class IndexHeaderManager : IIndexHeaderManager
{
    private readonly string _headerFilePath;
    private readonly ConcurrentDictionary<int, IndexMetadata> _metadata = new();
    private readonly ConcurrentDictionary<int, Pointer?> _rootPointers = new();
    private volatile bool _dirty;

    public IndexHeaderManager(string basePath)
    {
        _headerFilePath = Path.Combine(basePath, "index_headers.db");
        LoadHeaders();
    }

    public Task<Pointer?> GetRootPointerAsync(int indexId)
    {
        _rootPointers.TryGetValue(indexId, out var pointer);
        return Task.FromResult(pointer);
    }

    public Task SetRootPointerAsync(int indexId, Pointer pointer)
    {
        _rootPointers[indexId] = pointer;
        _dirty = true;
        return Task.CompletedTask;
    }

    public Task<IndexMetadata?> GetMetadataAsync(int indexId)
    {
        _metadata.TryGetValue(indexId, out var meta);
        return Task.FromResult(meta);
    }

    public Task SaveMetadataAsync(int indexId, IndexMetadata metadata)
    {
        metadata.ModifiedAt = DateTime.UtcNow;
        _metadata[indexId] = metadata;
        _dirty = true;
        return Task.CompletedTask;
    }

    public async Task DeleteIndexAsync(int indexId)
    {
        _rootPointers.TryRemove(indexId, out _);
        _metadata.TryRemove(indexId, out _);
        await SaveHeaders();
    }

    public async Task FlushAsync()
    {
        if (!_dirty)
            return;

        _dirty = false;
        await SaveHeaders();
    }

    private void LoadHeaders()
    {
        if (!File.Exists(_headerFilePath)) return;

        var bytes = File.ReadAllBytes(_headerFilePath);
        var offset = 0;

        if (bytes.Length < sizeof(int))
            return;

        // Read count
        var count = BitConverter.ToInt32(bytes, offset);
        offset += 4;

        for (var i = 0; i < count; i++)
        {
            if (offset + sizeof(int) + sizeof(byte) > bytes.Length)
                break;

            var indexId = BitConverter.ToInt32(bytes, offset);
            offset += 4;

            var hasPointer = bytes[offset++] != 0;
            if (hasPointer)
            {
                if (offset + Pointer.ByteSize > bytes.Length)
                    break;

                var pointer = Pointer.FromBytes(bytes, offset);
                _rootPointers[indexId] = pointer;
                offset += Pointer.ByteSize;
            }

            // Read metadata
            if (offset + MetadataByteSize > bytes.Length)
                break;

            var metadata = DeserializeMetadata(bytes, ref offset);
            _metadata[indexId] = metadata;
        }
    }

    private async Task SaveHeaders()
    {
        var buffer = new MemoryStream();
        var writer = new BinaryWriter(buffer);

        var allIndexIds = _rootPointers.Keys
            .Union(_metadata.Keys)
            .OrderBy(id => id)
            .ToList();

        writer.Write(allIndexIds.Count);

        foreach (var indexId in allIndexIds)
        {
            writer.Write(indexId);

            var hasPointer = _rootPointers.TryGetValue(indexId, out var pointer) && pointer != null;
            writer.Write(hasPointer ? (byte)1 : (byte)0);

            if (hasPointer)
                writer.Write(pointer!.Value.ToBytes());

            if (_metadata.TryGetValue(indexId, out var metadata))
                SerializeMetadata(writer, metadata);
            else
                SerializeMetadata(writer, new IndexMetadata
                {
                    IndexId = indexId,
                    Version = 1,
                    Degree = 0,
                    TotalNodes = 0,
                    CreatedAt = DateTime.UtcNow,
                    ModifiedAt = DateTime.UtcNow
                });
        }

        await File.WriteAllBytesAsync(_headerFilePath, buffer.ToArray());
    }

    private IndexMetadata DeserializeMetadata(byte[] bytes, ref int offset)
    {
        var indexId = BitConverter.ToInt32(bytes, offset);
        offset += 4;
        var version = BitConverter.ToInt32(bytes, offset);
        offset += 4;
        var degree = BitConverter.ToInt32(bytes, offset);
        offset += 4;
        var totalNodes = BitConverter.ToInt64(bytes, offset);
        offset += 8;
        var createdAt = new DateTime(BitConverter.ToInt64(bytes, offset));
        offset += 8;
        var modifiedAt = new DateTime(BitConverter.ToInt64(bytes, offset));
        offset += 8;

        return new IndexMetadata
        {
            IndexId = indexId,
            Version = version,
            Degree = degree,
            TotalNodes = totalNodes,
            CreatedAt = createdAt,
            ModifiedAt = modifiedAt
        };
    }

    private void SerializeMetadata(BinaryWriter writer, IndexMetadata meta)
    {
        writer.Write(meta.IndexId);
        writer.Write(meta.Version);
        writer.Write(meta.Degree);
        writer.Write(meta.TotalNodes);
        writer.Write(meta.CreatedAt.Ticks);
        writer.Write(meta.ModifiedAt.Ticks);
    }

    private const int MetadataByteSize =
        sizeof(int) + // IndexId
        sizeof(int) + // Version
        sizeof(int) + // Degree
        sizeof(long) + // TotalNodes
        sizeof(long) + // CreatedAt
        sizeof(long); // ModifiedAt
}
