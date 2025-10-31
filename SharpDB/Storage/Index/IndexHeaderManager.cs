using SharpDB.Core.Abstractions;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Index;

// Header Binary Layout (33 bytes):
//
//     ┌──────────────┬─────────┬─────────┬──────────┬─────────────┐
//     │ Root Pointer │ Degree  │ KeySize │ValueSize │ LastAutoKey │
//     │   13 bytes   │ 4 bytes │ 4 bytes │ 4 bytes  │  8 bytes    │
//     └──────────────┴─────────┴─────────┴──────────┴─────────────┘
//        Bytes 0-12     13-16     17-20     21-24       25-32

public record IndexHeader(
    Pointer? RootPointer,
    int Degree,
    int KeySize,
    int ValueSize,
    long LastAutoIncrementKey
);
public class IndexHeaderManager
{
    public const int HeaderSize = 33;

    public static async Task<IndexHeader> ReadHeaderAsync(
        int indexId,
        IIndexStorageManager storage)
    {
        var bytes = await storage.ReadHeaderAsync(indexId);

        if (bytes.Length < HeaderSize)
            return new IndexHeader(null, 0, 0, 0, 0);

        Pointer? rootPointer = null;
        if (bytes[0] != 0)
            rootPointer = Pointer.FromBytes(bytes, 0);
        
        var degree = BitConverter.ToInt32(bytes, 13);
        var keySize = BitConverter.ToInt32(bytes, 17);
        var valueSize = BitConverter.ToInt32(bytes, 21);
        var lastAutoKey = BitConverter.ToInt64(bytes, 25);
        
        return new IndexHeader(rootPointer, degree, keySize, valueSize, lastAutoKey);
    }
    
    public static async Task WriteHeaderAsync(
        int indexId,
        IndexHeader header,
        IIndexStorageManager storage)
    {
        var bytes = new byte[HeaderSize];

        header.RootPointer?.FillBytes(bytes, 0);

        BitConverter.GetBytes(header.Degree).CopyTo(bytes, 13);
        BitConverter.GetBytes(header.KeySize).CopyTo(bytes, 17);
        BitConverter.GetBytes(header.ValueSize).CopyTo(bytes, 21);
        BitConverter.GetBytes(header.LastAutoIncrementKey).CopyTo(bytes, 25);
        
        await storage.WriteHeaderAsync(indexId, bytes);
    }
    
    public static async Task UpdateRootPointerAsync(
        int indexId,
        Pointer rootPointer,
        IIndexStorageManager storage)
    {
        var header = await ReadHeaderAsync(indexId, storage);
        header = header with { RootPointer = rootPointer };
        await WriteHeaderAsync(indexId, header, storage);
    }

    public static async Task UpdateAutoIncrementAsync(
        int indexId,
        long newValue,
        IIndexStorageManager storage)
    {
        var header = await ReadHeaderAsync(indexId, storage);
        header = header with { LastAutoIncrementKey = newValue };
        await WriteHeaderAsync(indexId, header, storage);
    }
}