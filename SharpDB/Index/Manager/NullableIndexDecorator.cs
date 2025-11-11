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
public class NullableKeyIndex<TK, TV>
    where TK : struct, IComparable<TK>
{
    private readonly IUniqueTreeIndexManager<TK, TV> _innerIndex;
    private readonly Bitmap _nullBitmap;
    private readonly Dictionary<int, TV> _nullValues;
    private int _nextNullId;

    public NullableKeyIndex(
        IUniqueTreeIndexManager<TK, TV> innerIndex,
        int nullCapacity = 1000)
    {
        _innerIndex = innerIndex;
        _nullBitmap = new Bitmap(nullCapacity);
        _nullValues = new Dictionary<int, TV>();
        _nextNullId = 0;
    }

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

        return await _innerIndex.GetAsync(key.Value);
    }

    public async Task PutAsync(TK? key, TV value)
    {
        if (key == null)
        {
            // Store in null tracking
            var id = Interlocked.Increment(ref _nextNullId);
            _nullBitmap.Set(id);
            _nullValues[id] = value;
        }
        else
        {
            await _innerIndex.PutAsync(key.Value, value);
        }
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

        return await _innerIndex.RemoveAsync(key.Value);
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

        return await _innerIndex.ContainsKeyAsync(key.Value);
    }

    public async Task<int> CountAsync()
    {
        var count = await _innerIndex.CountAsync();
        
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
        return _innerIndex.FlushAsync();
    }

    public void Dispose()
    {
        _innerIndex.Dispose();
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
