using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;

namespace SharpDB.Storage.Index;

/// <summary>
/// File-based index storage with page management.
/// </summary>
public class DiskPageFileIndexStorageManager: IIndexStorageManager
{
    private readonly string _basePath;
    private readonly int _pageSize;
    private readonly Dictionary<int, FileStream> _indexFiles = new();
    private readonly SemaphoreSlim _lock = new(1, 1);
    
    public DiskPageFileIndexStorageManager(string basePath, int pageSize = 4096)
    {
        _basePath = basePath;
        _pageSize = pageSize;
        Directory.CreateDirectory(_basePath);
    }

    #region Header Operations

    public async Task<byte[]> ReadHeaderAsync(int indexId)
    {
        var file = await GetOrCreateFileAsync(indexId);
        
        await _lock.WaitAsync();
        try
        {
            // Header is always first page
            file.Seek(0, SeekOrigin.Begin);
            
            var buffer = new byte[IndexHeaderManager.HeaderSize];
            var bytesRead = await file.ReadAsync(buffer, 0, buffer.Length);
            
            if (bytesRead == 0)
            {
                // New file - return empty header
                return new byte[IndexHeaderManager.HeaderSize];
            }
            
            return buffer;
        }
        finally
        {
            _lock.Release();
        }    
    }
    
    public async Task WriteHeaderAsync(int indexId, byte[] header)
    {
        var file = await GetOrCreateFileAsync(indexId);
        
        await _lock.WaitAsync();
        try
        {
            // Write to first page
            file.Seek(0, SeekOrigin.Begin);
            await file.WriteAsync(header, 0, header.Length);
            await file.FlushAsync();
        }
        finally
        {
            _lock.Release();
        }
    }
    
    public Task<int> GetIndexHeaderSizeAsync(int indexId)
    {
        return Task.FromResult(IndexHeaderManager.HeaderSize);
    }

    #endregion

    #region Node Operations
    
    public async Task<Pointer> WriteAsync(int indexId, byte[] data)
    {
        var file = await GetOrCreateFileAsync(indexId);
        
        await _lock.WaitAsync();
        try
        {
            // Find next available position
            // Skip page 0 (reserved for header)
            long position = Math.Max(file.Length, _pageSize);
            
            // Align to page boundary
            if (position % _pageSize != 0)
            {
                position = ((position / _pageSize) + 1) * _pageSize;
            }
            
            file.Seek(position, SeekOrigin.Begin);
            await file.WriteAsync(data, 0, data.Length);
            await file.FlushAsync();
            
            return new Pointer(Pointer.TypeNode, position, 0);
        }
        finally
        {
            _lock.Release();
        }
    }
    
    public async Task<byte[]> ReadAsync(int indexId, Pointer pointer, int length)
    {
        var file = await GetOrCreateFileAsync(indexId);
        
        await _lock.WaitAsync();
        try
        {
            file.Seek(pointer.Position, SeekOrigin.Begin);
            
            var buffer = new byte[length];
            int bytesRead = await file.ReadAsync(buffer, 0, length);
            
            if (bytesRead < length)
            {
                throw new InvalidOperationException(
                    $"Expected {length} bytes, read {bytesRead}"
                );
            }
            
            return buffer;
        }
        finally
        {
            _lock.Release();
        }
    }
    
    public async Task UpdateAsync(int indexId, Pointer pointer, byte[] data)
    {
        var file = await GetOrCreateFileAsync(indexId);
        
        await _lock.WaitAsync();
        try
        {
            file.Seek(pointer.Position, SeekOrigin.Begin);
            await file.WriteAsync(data, 0, data.Length);
            await file.FlushAsync();
        }
        finally
        {
            _lock.Release();
        }
    }
    
    public async Task RemoveAsync(int indexId, Pointer pointer)
    {
        // In real implementation, maintain free list
        // For now, just mark as deleted (write zeros)
        var file = await GetOrCreateFileAsync(indexId);
        
        await _lock.WaitAsync();
        try
        {
            file.Seek(pointer.Position, SeekOrigin.Begin);
            
            // Write zeros to mark as deleted
            var zeros = new byte[_pageSize];
            await file.WriteAsync(zeros, 0, zeros.Length);
            await file.FlushAsync();
        }
        finally
        {
            _lock.Release();
        }
    }

    #endregion

    #region File Management
    
    private async Task<FileStream> GetOrCreateFileAsync(int indexId)
    {
        await _lock.WaitAsync();
        try
        {
            if (_indexFiles.TryGetValue(indexId, out var file))
            {
                return file;
            }
            
            var filePath = GetIndexFilePath(indexId);
            file = new FileStream(
                filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 8192,
                useAsync: true
            );
            
            _indexFiles[indexId] = file;
            return file;
        }
        finally
        {
            _lock.Release();
        }
    }
    
    private string GetIndexFilePath(int indexId)
    {
        return Path.Combine(_basePath, $"index_{indexId}.idx");
    }

    #endregion

    public byte[] GetEmptyNode(int keySize, int valueSize, int degree)
    {
        // Calculate node size
        var leafSize = 1 + (degree - 1) * (keySize + valueSize) + (2 * Pointer.ByteSize);
        
        // Internal: 1 + degree*PointerSize + (degree-1)*keySize
        var internalSize = 1 + (degree * Pointer.ByteSize) + ((degree - 1) * keySize);
        
        // Use max size
        var nodeSize = Math.Max(leafSize, internalSize);
        
        // Pad to page size
        if (nodeSize > _pageSize)
            throw new InvalidOperationException($"Node size {nodeSize} exceeds page size {_pageSize}");
        
        return new byte[nodeSize];
    }
    
    public async Task FlushAsync()
    {
        await _lock.WaitAsync();
        try
        {
            foreach (var file in _indexFiles.Values)
            {
                await file.FlushAsync();
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    public void Dispose()
    {
        foreach (var file in _indexFiles.Values)
        {
            file?.Dispose();
        }
        
        _indexFiles.Clear();
        _lock?.Dispose();
    }
}