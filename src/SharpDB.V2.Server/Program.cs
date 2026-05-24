using SharpDB.V2;
using SharpDB.V2.Server;

var options = new DatabaseOptions
{
    DataFilePath = "sharpdb.data",
    WalFilePath = "sharpdb.wal",
    Port = 5432
};

await using var server = new DatabaseServer(options);
await server.StartAsync();
