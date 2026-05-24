using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Index;

public interface IUniqueIndex<TKey> : IIndex<TKey>
{
    bool TryGet(TKey key, out PageId value, IOperationContext ctx);
    void Upsert(TKey key, PageId value, IOperationContext ctx);
}
