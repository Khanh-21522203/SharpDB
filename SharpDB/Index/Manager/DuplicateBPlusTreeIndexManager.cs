using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Serialization;
using SharpDB.Core.Abstractions.Storage;
using SharpDB.DataStructures;
using SharpDB.Serialization;

namespace SharpDB.Index.Manager;

/// <summary>
/// Index manager that supports multiple values per key.
/// Uses ValueList internally to store multiple values.
/// </summary>
public class DuplicateBPlusTreeIndexManager<TK, TV> : IDuplicateTreeIndexManager<TK, TV>
    where TK : IComparable<TK>
    where TV : IComparable<TV>
{
    private readonly IUniqueTreeIndexManager<TK, BinaryList<TV>> _inner;
    private readonly ISerializer<TV> _valueSerializer;
    
    public DuplicateBPlusTreeIndexManager(
        IIndexStorageManager storage,
        int indexId,
        int degree,
        ISerializer<TK> keySerializer,
        ISerializer<TV> valueSerializer)
    {
        _valueSerializer = valueSerializer;
        var binaryListSerializer = new BinaryListSerializer<TV>(valueSerializer);
        
        //TODO: binaryListSerializer
        
        // Create inner unique index with BinaryList as value type
        _inner = new BPlusTreeIndexManager<TK, BinaryList<TV>>(
            storage,
            indexId,
            degree);
    }
    
    // IDuplicateTreeIndexManager methods
    public async Task<List<TV>> GetAllAsync(TK key)
    {
        var binaryList = await _inner.GetAsync(key);
        return binaryList?.ToList() ?? new List<TV>();
    }
    
    public async Task<int> CountAsync(TK key)
    {
        var binaryList = await _inner.GetAsync(key);
        return binaryList?.Count ?? 0;
    }
    
    // ITreeIndexManager methods
    public async Task<TV?> GetAsync(TK key)
    {
        var binaryList = await _inner.GetAsync(key);
        return binaryList != null && binaryList.Count > 0 
            ? binaryList[0] 
            : default;
    }
    
    public async Task PutAsync(TK key, TV value)
    {
        var existing = await _inner.GetAsync(key);
        
        if (existing == null)
        {
            // Create new list with single value
            var list = new BinaryList<TV>(_valueSerializer);
            list.Add(value);
            await _inner.PutAsync(key, list);
        }
        else
        {
            // Append to existing list
            existing.Add(value);
            await _inner.PutAsync(key, existing);
        }
    }
    
    public async Task<bool> RemoveAsync(TK key)
    {
        // Remove all values for this key
        return await _inner.RemoveAsync(key);
    }
    
    public async Task<bool> RemoveValueAsync(TK key, TV value)
    {
        var existing = await _inner.GetAsync(key);
        if (existing == null)
            return false;
        
        bool removed = existing.Remove(value);
        
        if (removed)
        {
            if (existing.Count == 0)
            {
                // Remove key if no values left
                await _inner.RemoveAsync(key);
            }
            else
            {
                // Update with remaining values
                await _inner.PutAsync(key, existing);
            }
        }
        
        return removed;
    }
    
    public Task FlushAsync() => _inner.FlushAsync();
    
    public void Dispose() => _inner.Dispose();
}