using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Transactions;

public interface IVersionManager
{
    void WriteVersion(PageId page, scoped in ReadOnlySpan<byte> data, ITransaction tx);
    bool TryReadVersion(PageId page, TransactionId readerId, out ReadOnlySpan<byte> data);
    void Purge(TransactionId oldestActiveId);
}
