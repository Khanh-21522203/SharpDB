using SharpDB.Core.Abstractions.Sessions;
using SharpDB.DataStructures;
using SharpDB.Index.Node;
using SharpDB.Utils.Caching;

namespace SharpDB.Storage.Index.Sessions;

public class CachedIndexIoSession<TK>(
    IIndexIoSession<TK> innerSession,
    int cacheCapacity = 1000) : IIndexIoSession<TK>
    where TK : IComparable<TK>
{
    private readonly LruCache<Pointer, TreeNode<TK>> _cache = new(cacheCapacity);

    public Task<TreeNode<TK>> GetRootAsync()
    {
        return innerSession.GetRootAsync();
    }
    
    public async Task<TreeNode<TK>> ReadAsync(Pointer pointer)
    {
        // Try cache first
        if (_cache.TryGet(pointer, out var cachedNode))
        {
            return cachedNode;
        }
        
        // Read from storage
        var node = await innerSession.ReadAsync(pointer);
        
        // Cache it
        _cache.Put(pointer, node);
        
        return node;
    }
    
    public async Task<Pointer> WriteAsync(TreeNode<TK> node)
    {
        var pointer = await innerSession.WriteAsync(node);
        
        // Cache newly written node
        _cache.Put(pointer, node);
        
        return pointer;
    }
    
    public Task UpdateAsync(TreeNode<TK> node)
    {
        if (node.Pointer != null)
        {
            // Update cache
            _cache.Put(node.Pointer, node);
        }
        
        return innerSession.UpdateAsync(node);
    }
    
    public Task RemoveAsync(TreeNode<TK> node)
    {
        if (node.Pointer != null)
        {
            // Remove from cache
            _cache.Remove(node.Pointer);
        }
        
        return innerSession.RemoveAsync(node);
    }
    
    public Task CommitAsync()
    {
        return innerSession.CommitAsync();
    }
    
    public void Dispose()
    {
        _cache.Clear();
        innerSession.Dispose();
    }
}