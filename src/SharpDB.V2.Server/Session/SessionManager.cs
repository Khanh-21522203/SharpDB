namespace SharpDB.V2.Server.Session;

public sealed class SessionManager : IDisposable
{
    public void Register(ClientSession session) { }
    public Task ShutdownAsync() => Task.CompletedTask;
    public void Dispose() { }
}
