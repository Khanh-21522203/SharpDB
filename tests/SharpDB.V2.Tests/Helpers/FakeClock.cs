namespace SharpDB.V2.Tests.Helpers;
using SharpDB.V2.Infrastructure.Time;

public sealed class FakeClock : ISystemClock
{
    public DateTimeOffset UtcNow => throw new NotImplementedException();
}
