using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Storage;
using SharpDB.V2.Engine.Abstractions.Transactions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Operations;

public sealed class OperationContext : IOperationContext
{
    public OperationContextId Id => throw new NotImplementedException();

    public ITransaction Transaction => throw new NotImplementedException();

    public IPageBuffer PageBuffer => throw new NotImplementedException();

    public bool IsReadOnly => throw new NotImplementedException();

    public void Dispose() { }
}
