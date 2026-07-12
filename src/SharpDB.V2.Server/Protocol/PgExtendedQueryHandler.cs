namespace SharpDB.V2.Server.Protocol;

public sealed class PgExtendedQueryHandler
{
    public PgExtendedQueryHandler(PgMessageReader reader, PgMessageWriter writer) { }
    public Task HandleAsync(CancellationToken ct = default) => Task.CompletedTask;
}
