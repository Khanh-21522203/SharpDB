namespace SharpDB.V2.Tests.Helpers;
using Xunit;

public sealed class TempDatabaseFixture : IAsyncLifetime
{
    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.CompletedTask;
}
