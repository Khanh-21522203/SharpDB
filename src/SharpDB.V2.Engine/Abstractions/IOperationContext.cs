using SharpDB.V2.Engine.Abstractions.Storage;
using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions;

public interface IOperationContext : IDisposable
{
    OperationContextId Id { get; }
    ITransaction Transaction { get; }
    IPageBuffer PageBuffer { get; }
    bool IsReadOnly { get; }
}
