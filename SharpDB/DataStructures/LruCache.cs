namespace SharpDB.Utils.Caching;

public class LruCache<TKey, TValue> where TKey : notnull
{
    private readonly int _capacity;
    private readonly Dictionary<TKey, LinkedListNode<CacheEntry>> _dict;
    private readonly LinkedList<CacheEntry> _list;
    private readonly Lock _lock = new();
    
    private record CacheEntry(TKey Key, TValue Value);
    
    public LruCache(int capacity)
    {
        if (capacity <= 0)
            throw new ArgumentException("Capacity must be positive");
        
        _capacity = capacity;
        _dict = new Dictionary<TKey, LinkedListNode<CacheEntry>>(capacity);
        _list = new LinkedList<CacheEntry>();
    }

    public bool TryGet(TKey key, out TValue value)
    {
        lock (_lock)
        {
            if (_dict.TryGetValue(key, out var node))
            {
                _list.Remove(node);
                _list.AddFirst(node);
                
                value = node.Value.Value;
                return true;
            }
            
            value = default!;
            return false;
        }
    }

    public void Put(TKey key, TValue value)
    {
        lock (_lock)
        {
            if (_dict.TryGetValue(key, out var existingNode))
            {
                _list.Remove(existingNode);
                _list.AddFirst(existingNode);
                
                var newEntry = new CacheEntry(key, value);
                existingNode.Value = newEntry;
            }
            else
            {
                if (_dict.Count >= _capacity)
                {
                    var lruNode = _list.Last!;
                    _list.RemoveLast();
                    _dict.Remove(lruNode.Value.Key);
                }
                
                var entry = new CacheEntry(key, value);
                var node = _list.AddFirst(entry);
                _dict[key] = node;
            }
        }
    }

    public bool Remove(TKey key)
    {
        lock (_lock)
        {
            if (_dict.TryGetValue(key, out var node))
            {
                _list.Remove(node);
                _dict.Remove(key);
                return true;
            }
            
            return false;
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            _dict.Clear();
            _list.Clear();
        }
    }

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _dict.Count;
            }
        }
    }
}