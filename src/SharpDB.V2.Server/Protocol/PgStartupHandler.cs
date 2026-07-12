namespace SharpDB.V2.Server.Protocol;

public sealed class PgStartupHandler
{
    public PgStartupHandler(PgMessageReader reader, PgMessageWriter writer) { }
    public Task HandleAsync(CancellationToken ct = default) => Task.CompletedTask;
}
