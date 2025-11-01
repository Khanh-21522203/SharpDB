using System.IO.Compression;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Page;

namespace SharpDB.Storage.Database;

/// <summary>
/// Decorator that adds compression to database storage.
/// </summary>
public class CompressedDatabaseStorageManager(
    IDatabaseStorageManager inner,
    CompressionLevel compressionLevel = CompressionLevel.Fastest,
    int compressionThreshold = 512)
    : IDatabaseStorageManager
{
    private readonly IDatabaseStorageManager _inner = inner;

    public async Task<Pointer> StoreAsync(int schemeId, int collectionId, int version, byte[] data)
    {
        var dataToStore = data.Length >= compressionThreshold
            ? Compress(data)
            : data;
        
        return await _inner.StoreAsync(schemeId, collectionId, version, dataToStore);
    }
    
    public async Task<DBObject?> SelectAsync(Pointer pointer)
    {
        var dbObject = await _inner.SelectAsync(pointer);
        
        if (dbObject == null)
            return null;
        
        // Decompress if needed
        var data = dbObject.Data;
        if (IsCompressed(data))
        {
            var decompressed = Decompress(data);
            dbObject.ModifyData(decompressed);
        }
        
        return dbObject;
    }
    
    public Task UpdateAsync(Pointer pointer, byte[] data)
    {
        var dataToStore = data.Length >= compressionThreshold
            ? Compress(data)
            : data;
        
        return _inner.UpdateAsync(pointer, dataToStore);
    }
    
    public Task DeleteAsync(Pointer pointer) => _inner.DeleteAsync(pointer);
    
    public IAsyncEnumerable<DBObject> ScanAsync(int collectionId) => _inner.ScanAsync(collectionId);
    
    public Task FlushAsync() => _inner.FlushAsync();
    
    public void Dispose() => _inner.Dispose();
    
    private byte[] Compress(byte[] data)
    {
        using var output = new MemoryStream();
        output.WriteByte(0x1F); // Compression marker
        output.WriteByte(0x8B); // GZip marker
        
        using (var gzip = new GZipStream(output, compressionLevel))
        {
            gzip.Write(data, 0, data.Length);
        }
        
        return output.ToArray();
    }
    
    private byte[] Decompress(byte[] data)
    {
        using var input = new MemoryStream(data, 2, data.Length - 2); // Skip markers
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        
        gzip.CopyTo(output);
        return output.ToArray();
    }
    
    private bool IsCompressed(byte[] data)
    {
        return data.Length >= 2 && data[0] == 0x1F && data[1] == 0x8B;
    }
}