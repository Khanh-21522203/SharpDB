using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Core.Abstractions.Sessions;

/// <summary>
/// Provides transactional access to B+ Tree nodes.
/// </summary>
public interface IIndexIoSession<TK> : IDisposable
    where TK : IComparable<TK>
{
    /// <summary>
    /// Get root node (creates if doesn't exist).
    /// </summary>
    Task<TreeNode<TK>> GetRootAsync();
    
    /// <summary>
    /// Read node at pointer.
    /// </summary>
    Task<TreeNode<TK>> ReadAsync(Pointer pointer);
    
    /// <summary>
    /// Write new node (assigns pointer).
    /// </summary>
    Task<Pointer> WriteAsync(TreeNode<TK> node);
    
    /// <summary>
    /// Update existing node.
    /// </summary>
    Task UpdateAsync(TreeNode<TK> node);
    
    /// <summary>
    /// Remove node.
    /// </summary>
    Task RemoveAsync(TreeNode<TK> node);
    
    /// <summary>
    /// Commit all changes.
    /// </summary>
    Task CommitAsync();
}