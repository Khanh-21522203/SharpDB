namespace SharpDB.V2.Server;
using SharpDB.V2.Server.Session;

public sealed class DatabaseServer : IAsyncDisposable
{
    public DatabaseServer(SharpDB.V2.DatabaseOptions options) { }
    public Task StartAsync(CancellationToken ct = default) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
