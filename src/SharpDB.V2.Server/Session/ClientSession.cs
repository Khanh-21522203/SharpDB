namespace SharpDB.V2.Server.Session;

public sealed class ClientSession : IDisposable
{
    public ClientSession(System.Net.Sockets.TcpClient client) { }
    public Task RunAsync(CancellationToken ct = default) => Task.CompletedTask;
    public void Dispose() { }
}
