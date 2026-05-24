namespace SharpDB.V2;

public sealed class DatabaseOptions
{
    public string DataFilePath { get; init; } = "";
    public string WalFilePath { get; init; } = "";
    public int BufferPoolCapacity { get; init; } = 1024;
    public int Port { get; init; } = 5432;
}
