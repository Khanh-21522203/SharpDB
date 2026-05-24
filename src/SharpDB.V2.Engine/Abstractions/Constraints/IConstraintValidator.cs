using SharpDB.V2.Engine.Abstractions;

namespace SharpDB.V2.Engine.Abstractions.Constraints;

public interface IConstraintValidator<T>
{
    void ValidateInsert(T value, IOperationContext ctx);
    void ValidateUpdate(T oldValue, T newValue, IOperationContext ctx);
    void ValidateDelete(T value, IOperationContext ctx);
}
