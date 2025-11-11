using SharpDB.Core.Abstractions.Index;
using SharpDB.DataStructures;

namespace SharpDB.Index.Manager;

/// <summary>
/// Special wrapper for indexes that need to support nullable keys.
/// Uses a separate bitmap/dictionary for null entries to avoid type system constraints.
/// 
/// NOTE: This does NOT implement IUniqueTreeIndexManager due to C# nullable constraints.
/// Use as a standalone wrapper with manual key handling.
/// </summary>
public class NullableKeyIndex<TK, TV>(
    IUniqueTreeIndexManager<TK, TV> innerIndex,
    int initialNullCapacity = 1000)
    where TK : struct, IComparable<TK>
{
    private Bitmap _nullBitmap = new(initialNullCapacity);
    private readonly Dictionary<int, TV> _nullValues = new();
    private int _nextNullId = 0;
    private int _currentCapacity = initialNullCapacity;

    public async Task<TV?> GetAsync(TK? key)
    {
        if (key == null)
        {
            // Return first null value if exists
            foreach (var kvp in _nullValues)
            {
                if (_nullBitmap.IsSet(kvp.Key))
                    return kvp.Value;
            }
            return default;
        }

        return await innerIndex.GetAsync(key.Value);
    }

    public async Task PutAsync(TK? key, TV value)
    {
        if (key == null)
        {
            // Store in null tracking
            var id = Interlocked.Increment(ref _nextNullId);
            
            // Resize bitmap if needed
            if (id >= _currentCapacity)
            {
                ResizeBitmap(id);
            }
            
            _nullBitmap.Set(id);
            _nullValues[id] = value;
        }
        else
        {
            await innerIndex.PutAsync(key.Value, value);
        }
    }
    
    /// <summary>
    /// Dynamically resize bitmap when capacity exceeded
    /// </summary>
    private void ResizeBitmap(int requiredId)
    {
        // Double capacity until it fits
        var newCapacity = _currentCapacity;
        while (newCapacity <= requiredId)
        {
            newCapacity *= 2;
        }
        
        // Create new bitmap with larger capacity
        var newBitmap = new Bitmap(newCapacity);
        
        // Copy existing bits
        for (int i = 0; i < _currentCapacity; i++)
        {
            if (_nullBitmap.IsSet(i))
            {
                newBitmap.Set(i);
            }
        }
        
        _nullBitmap = newBitmap;
        _currentCapacity = newCapacity;
    }

    public async Task<bool> RemoveAsync(TK? key)
    {
        if (key == null)
        {
            // Remove all null entries
            var removed = false;
            foreach (var id in _nullValues.Keys.ToList())
            {
                if (_nullBitmap.IsSet(id))
                {
                    _nullBitmap.Clear(id);
                    _nullValues.Remove(id);
                    removed = true;
                }
            }
            return removed;
        }

        return await innerIndex.RemoveAsync(key.Value);
    }

    public async Task<bool> ContainsKeyAsync(TK? key)
    {
        if (key == null)
        {
            foreach (var id in _nullValues.Keys)
            {
                if (_nullBitmap.IsSet(id))
                    return true;
            }
            return false;
        }

        return await innerIndex.ContainsKeyAsync(key.Value);
    }

    public async Task<int> CountAsync()
    {
        var count = await innerIndex.CountAsync();
        
        // Add null entries
        foreach (var id in _nullValues.Keys)
        {
            if (_nullBitmap.IsSet(id))
                count++;
        }

        return count;
    }

    public Task FlushAsync()
    {
        return innerIndex.FlushAsync();
    }

    public void Dispose()
    {
        innerIndex.Dispose();
    }

    /// <summary>
    /// Get all values with null keys
    /// </summary>
    public List<TV> GetNullValues()
    {
        var results = new List<TV>();
        foreach (var kvp in _nullValues)
        {
            if (_nullBitmap.IsSet(kvp.Key))
                results.Add(kvp.Value);
        }
        return results;
    }

    /// <summary>
    /// Check if there are any null keys
    /// </summary>
    public bool HasNullKeys()
    {
        foreach (var id in _nullValues.Keys)
        {
            if (_nullBitmap.IsSet(id))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Remove all entries with null keys
    /// </summary>
    public int RemoveAllNulls()
    {
        var count = 0;
        foreach (var id in _nullValues.Keys.ToList())
        {
            if (_nullBitmap.IsSet(id))
            {
                _nullBitmap.Clear(id);
                _nullValues.Remove(id);
                count++;
            }
        }
        return count;
    }
}
