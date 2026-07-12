using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Constraints;

namespace SharpDB.V2.Engine.Constraints;

public sealed class CompositeConstraintValidator<T> : IConstraintValidator<T>
{
    public void ValidateInsert(T value, IOperationContext ctx) { }

    public void ValidateUpdate(T oldValue, T newValue, IOperationContext ctx) { }

    public void ValidateDelete(T value, IOperationContext ctx) { }
}
