namespace SharpDB.V2.Server.Protocol.Messages;

public sealed class AuthenticationOkMessage { }

public sealed class ParameterStatusMessage
{
    public string Name { get; init; } = "";
    public string Value { get; init; } = "";
}

public sealed class BackendKeyDataMessage
{
    public int ProcessId { get; init; }
    public int SecretKey { get; init; }
}

public sealed class ReadyForQueryMessage
{
    public char TransactionStatus { get; init; } = 'I';
}

public sealed class RowDescriptionMessage
{
    public IReadOnlyList<FieldDescription> Fields { get; init; } = [];
}

public sealed class FieldDescription
{
    public string Name { get; init; } = "";
    public int TypeOid { get; init; }
    public short TypeSize { get; init; }
}

public sealed class DataRowMessage
{
    public IReadOnlyList<byte[]?> ColumnValues { get; init; } = [];
}

public sealed class CommandCompleteMessage
{
    public string Tag { get; init; } = "";
}

public sealed class ErrorResponseMessage
{
    public string Message { get; init; } = "";
    public string Code { get; init; } = "XX000";
}
