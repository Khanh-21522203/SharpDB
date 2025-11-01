using SharpDB.DataStructures;
using SharpDB.Index.Node;

namespace SharpDB.Core.Abstractions.Sessions;

public interface IIndexIOSession<TK> : IDisposable where TK : IComparable<TK>
{
    Task<TreeNode<TK>> ReadAsync(Pointer pointer);
    Task<Pointer> WriteAsync(TreeNode<TK> node);
    Task FlushAsync();
}