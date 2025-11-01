using System.Collections.Concurrent;
using Serilog;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Storage.Header;

namespace SharpDB.Storage.Index;

/// <summary>
///     Disk-based storage manager for B+ tree nodes.
///     Uses file per index approach for isolation.
/// </summary>
public class DiskPageFileIndexStorageManager : IIndexStorageManager
{
    private readonly string _basePath;
    private readonly IFileHandlerPool _filePool;
    private readonly ConcurrentDictionary<int, HashSet<long>> _freePositions = new();
    private readonly IIndexHeaderManager _headerManager;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<int, long> _nextPositions = new();
    private bool _disposed;

    public DiskPageFileIndexStorageManager(
        string basePath,
        ILogger logger,
        IFileHandlerPool filePool)
    {
        _basePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
        _filePool = filePool;
        _headerManager = new IndexHeaderManager(basePath);
        _logger = logger;

        Directory.CreateDirectory(basePath);
    }

    public async Task<Pointer?> GetRootPointerAsync(int indexId)
    {
        return await _headerManager.GetRootPointerAsync(indexId);
    }

    public async Task SetRootPointerAsync(int indexId, Pointer pointer)
    {
        await _headerManager.SetRootPointerAsync(indexId, pointer);

        _logger.Debug("Set root pointer for index {IndexId}: {Pointer}",
            indexId, pointer);
    }

    public async Task<NodeData> ReadNodeAsync(int indexId, Pointer pointer)
    {
        if (pointer.Type != Pointer.TypeNode)
            throw new ArgumentException("Pointer must be of TypeNode", nameof(pointer));

        var filePath = GetIndexFilePath(indexId);
        var handle = await _filePool.GetHandleAsync(indexId, filePath);

        // Seek to position
        handle.Seek(pointer.Position, SeekOrigin.Begin);

        // Read node size (first 4 bytes)
        var sizeBuffer = new byte[4];
        await handle.ReadExactlyAsync(sizeBuffer, 0, 4);
        var nodeSize = BitConverter.ToInt32(sizeBuffer, 0);

        // Read full node
        var nodeData = new byte[nodeSize];
        Array.Copy(sizeBuffer, nodeData, 4);
        await handle.ReadExactlyAsync(nodeData, 4, nodeSize - 4);

        _logger.Debug("Read node at {Position}, size {Size}",
            pointer.Position, nodeSize);

        return new NodeData(pointer, nodeData);
    }

    public async Task<NodeData> WriteNewNodeAsync(int indexId, byte[] data)
    {
        if (data == null || data.Length == 0)
            throw new ArgumentException("Node data cannot be empty", nameof(data));

        var filePath = GetIndexFilePath(indexId);
        var handle = await _filePool.GetHandleAsync(indexId, filePath);

        // Try reuse free position
        long position;
        if (_freePositions.TryGetValue(indexId, out var freeSet) && freeSet.Count > 0)
        {
            position = freeSet.First();
            freeSet.Remove(position);

            _logger.Debug("Reusing free position {Position} for index {IndexId}",
                position, indexId);
        }
        else
        {
            // Allocate new position at end of file
            position = _nextPositions.GetOrAdd(indexId, handle.Length);
            _nextPositions[indexId] = position + data.Length;
        }

        // Write node
        handle.Seek(position, SeekOrigin.Begin);
        await handle.WriteAsync(data, 0, data.Length);
        await handle.FlushAsync();

        var pointer = new Pointer(Pointer.TypeNode, position, 0);

        _logger.Debug("Wrote new node for index {IndexId} at {Position}, size {Size}",
            indexId, position, data.Length);

        return new NodeData(pointer, data);
    }

    public async Task UpdateNodeAsync(int indexId, Pointer pointer, byte[] data)
    {
        if (pointer.Type != Pointer.TypeNode)
            throw new ArgumentException("Pointer must be of TypeNode", nameof(pointer));

        if (data == null || data.Length == 0)
            throw new ArgumentException("Node data cannot be empty", nameof(data));

        var filePath = GetIndexFilePath(indexId);
        var handle = await _filePool.GetHandleAsync(indexId, filePath);

        // Write at existing position
        handle.Seek(pointer.Position, SeekOrigin.Begin);
        await handle.WriteAsync(data, 0, data.Length);
        await handle.FlushAsync();

        _logger.Debug("Updated node at {Position}, size {Size}",
            pointer.Position, data.Length);
    }

    public Task RemoveNodeAsync(int indexId, Pointer pointer)
    {
        // Add to free list for reuse
        var freeSet = _freePositions.GetOrAdd(indexId, _ => new HashSet<long>());
        freeSet.Add(pointer.Position);

        _logger.Debug("Marked position {Position} as free for index {IndexId}",
            pointer.Position, indexId);

        return Task.CompletedTask;
    }

    public byte[] GetEmptyNode(int keySize, int valueSize, int degree)
    {
        // Calculate node size based on B+ tree requirements
        // Header: Type(1) + KeyCount(4) + IsRoot(1) = 6 bytes
        // Keys: degree * keySize
        // Values/Pointers: (degree + 1) * max(valueSize, Pointer.Bytes)

        var headerSize = 6;
        var keysSize = degree * keySize;
        var valuesSize = (degree + 1) * Math.Max(valueSize, Pointer.ByteSize);
        var totalSize = headerSize + keysSize + valuesSize;

        var buffer = new byte[totalSize + 4]; // +4 for size prefix
        BitConverter.GetBytes(totalSize).CopyTo(buffer, 0);

        return buffer;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _filePool.Dispose();

        _logger.Information("DiskPageFileIndexStorageManager disposed");
    }

    private string GetIndexFilePath(int indexId)
    {
        return Path.Combine(_basePath, $"index_{indexId}.dat");
    }
}