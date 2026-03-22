namespace SharpDB.Engine;

internal interface IVacuumable
{
    Task VacuumAsync();
}
