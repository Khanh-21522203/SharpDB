using SharpDB.Core.Abstractions.Index;
using SharpDB.Core.Abstractions.Sessions;
using SharpDB.Core.Abstractions.Storage;

namespace SharpDB.Index.Session;

public class IndexSessionFactory<TK>(
    IIndexStorageManager storage,
    INodeFactory<TK, object> nodeFactory,
    bool useBuffering = true)
    : IIndexSessionFactory<TK>
    where TK : IComparable<TK>
{
    public IIndexIOSession<TK> CreateSession(int indexId)
    {
        if (useBuffering)
            return new BufferedIndexIOSession<TK>(storage, nodeFactory, indexId);
        return new ImmediateCommitIndexSession<TK>(storage, nodeFactory, indexId);
    }
}