namespace SharpDB.V2.Server.Protocol;

public sealed class PgSimpleQueryHandler
{
    public PgSimpleQueryHandler(PgMessageReader reader, PgMessageWriter writer) { }
    public Task HandleAsync(CancellationToken ct = default) => Task.CompletedTask;
}
