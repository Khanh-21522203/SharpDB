using System.Collections.Concurrent;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Header;

public class IndexHeaderManager : IIndexHeaderManager
{
    private readonly string _headerFilePath;
    private readonly ConcurrentDictionary<int, Pointer?> _rootPointers = new();
    private readonly ConcurrentDictionary<int, IndexMetadata> _metadata = new();
    
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
    
    public async Task SetRootPointerAsync(int indexId, Pointer pointer)
    {
        _rootPointers[indexId] = pointer;
        await SaveHeaders();
    }
    
    public Task<IndexMetadata?> GetMetadataAsync(int indexId)
    {
        _metadata.TryGetValue(indexId, out var meta);
        return Task.FromResult(meta);
    }
    
    public async Task SaveMetadataAsync(int indexId, IndexMetadata metadata)
    {
        metadata.ModifiedAt = DateTime.UtcNow;
        _metadata[indexId] = metadata;
        await SaveHeaders();
    }
    
    public async Task DeleteIndexAsync(int indexId)
    {
        _rootPointers.TryRemove(indexId, out _);
        _metadata.TryRemove(indexId, out _);
        await SaveHeaders();
    }
    
    private void LoadHeaders()
    {
        if (!File.Exists(_headerFilePath)) return;
        
        var bytes = File.ReadAllBytes(_headerFilePath);
        var offset = 0;
        
        // Read count
        var count = BitConverter.ToInt32(bytes, offset);
        offset += 4;
        
        for (int i = 0; i < count; i++)
        {
            var indexId = BitConverter.ToInt32(bytes, offset);
            offset += 4;
            
            var hasPointer = bytes[offset++] != 0;
            if (hasPointer)
            {
                var pointer = Pointer.FromBytes(bytes, offset);
                _rootPointers[indexId] = pointer;
                offset += Pointer.ByteSize;
            }
            
            // Read metadata
            var metadata = DeserializeMetadata(bytes, ref offset);
            _metadata[indexId] = metadata;
        }
    }
    
    private async Task SaveHeaders()
    {
        var buffer = new MemoryStream();
        var writer = new BinaryWriter(buffer);
        
        writer.Write(_rootPointers.Count);
        
        foreach (var kvp in _rootPointers)
        {
            writer.Write(kvp.Key);
            writer.Write(kvp.Value != null ? (byte)1 : (byte)0);
            
            if (kvp.Value != null)
                writer.Write(kvp.Value.Value.ToBytes());
            
            if (_metadata.TryGetValue(kvp.Key, out var meta))
                SerializeMetadata(writer, meta);
        }
        
        await File.WriteAllBytesAsync(_headerFilePath, buffer.ToArray());
    }
    
    private IndexMetadata DeserializeMetadata(byte[] bytes, ref int offset)
    {
        return new IndexMetadata
        {
            IndexId = BitConverter.ToInt32(bytes, offset += 4),
            Version = BitConverter.ToInt32(bytes, offset += 4),
            Degree = BitConverter.ToInt32(bytes, offset += 4),
            TotalNodes = BitConverter.ToInt64(bytes, offset += 8),
            CreatedAt = new DateTime(BitConverter.ToInt64(bytes, offset += 8)),
            ModifiedAt = new DateTime(BitConverter.ToInt64(bytes, offset += 8))
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
}