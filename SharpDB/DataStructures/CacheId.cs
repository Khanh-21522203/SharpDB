namespace SharpDB.DataStructures;

/// <summary>
///     Cache key combining index ID and pointer.
///     Used in LRU cache for node caching.
/// </summary>
public record CacheId(int IndexId, Pointer Pointer)
{
    public override string ToString()
    {
        return $"Cache({IndexId}, {Pointer})";
    }
}