using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Vacuum;

public interface IVacuumEngine
{
    VacuumResult Run(string collectionName, IOperationContext ctx);
    long EstimateReclaimable(string collectionName);
}
