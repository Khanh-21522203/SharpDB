using SharpDB.Core.Abstractions.Index;
using SharpDB.DataStructures;

namespace SharpDB.Index.Manager;

public class NullableIndexManager<TV>(IUniqueTreeIndexManager<long, TV> innerIndex, int nullBitmapCapacity = 10000)
    : INullableIndex<TV>
{
    private readonly Bitmap _nullBitmap = new(nullBitmapCapacity);
    private long _nextId = 0;

    public async Task<TV?> GetAsync(object? key)
    {
        if (key == null)
        {
            var nulls = await GetNullsAsync();
            return nulls.FirstOrDefault();
        }
        
        return await innerIndex.GetAsync(GetKeyHash(key));
    }
    
    public async Task PutAsync(object? key, TV value)
    {
        if (key == null)
        {
            var id = Interlocked.Increment(ref _nextId);
            _nullBitmap.Set((int)id);
            await innerIndex.PutAsync(id, value);
        }
        else
        {
            await innerIndex.PutAsync(GetKeyHash(key), value);
        }
    }
    
    public async Task<bool> RemoveAsync(object? key)
    {
        if (key == null)
        {
            var removed = await RemoveNullsAsync();
            return removed > 0;
        }
        
        return await innerIndex.RemoveAsync(GetKeyHash(key));
    }
    
    public async Task<List<TV>> GetNullsAsync()
    {
        var results = new List<TV>();
        for (var i = 0; i < _nullBitmap.Capacity; i++)
        {
            if (_nullBitmap.IsSet(i))
            {
                var value = await innerIndex.GetAsync(i);
                if (value != null)
                    results.Add(value);
            }
        }
        return results;
    }
    
    public Task<bool> HasNullsAsync()
    {
        for (var i = 0; i < _nullBitmap.Capacity; i++)
        {
            if (_nullBitmap.IsSet(i))
                return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }
    
    public async Task<int> RemoveNullsAsync()
    {
        var count = 0;
        for (var i = 0; i < _nullBitmap.Capacity; i++)
        {
            if (_nullBitmap.IsSet(i))
            {
                await innerIndex.RemoveAsync(i);
                _nullBitmap.Clear(i);
                count++;
            }
        }
        return count;
    }
    
    private long GetKeyHash(object key)
    {
        return key.GetHashCode();
    }
}