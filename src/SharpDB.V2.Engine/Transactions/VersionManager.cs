using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Transactions;

public sealed class VersionManager : IVersionManager
{
    public void WriteVersion(PageId page, scoped in ReadOnlySpan<byte> data, ITransaction tx) { }

    public bool TryReadVersion(PageId page, TransactionId readerId, out ReadOnlySpan<byte> data)
    {
        data = default;
        return false;
    }

    public void Purge(TransactionId oldestActiveId) { }
}
