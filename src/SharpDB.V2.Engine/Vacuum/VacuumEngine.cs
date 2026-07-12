using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Vacuum;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Vacuum;

public sealed class VacuumEngine : IVacuumEngine
{
    public VacuumResult Run(string collectionName, IOperationContext ctx) => throw new NotImplementedException();

    public long EstimateReclaimable(string collectionName) => throw new NotImplementedException();
}
