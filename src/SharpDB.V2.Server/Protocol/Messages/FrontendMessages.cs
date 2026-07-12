namespace SharpDB.V2.Server.Protocol.Messages;

public sealed class StartupMessage
{
    public int ProtocolVersion { get; init; }
    public IReadOnlyDictionary<string, string> Parameters { get; init; } = new Dictionary<string, string>();
}

public sealed class QueryMessage
{
    public string Query { get; init; } = "";
}

public sealed class ParseMessage
{
    public string StatementName { get; init; } = "";
    public string Query { get; init; } = "";
}

public sealed class BindMessage
{
    public string PortalName { get; init; } = "";
    public string StatementName { get; init; } = "";
}

public sealed class ExecuteMessage
{
    public string PortalName { get; init; } = "";
    public int MaxRows { get; init; }
}

public sealed class SyncMessage { }

public sealed class TerminateMessage { }
