#pragma warning disable CS0414
namespace SharpDB.V2;
using SharpDB.V2.Engine.Operations;
using SharpDB.V2.Engine.Serialization;
using SharpDB.V2.Engine.Storage;
using SharpDB.V2.Engine.Transactions;
using SharpDB.V2.Engine.Wal;
using SharpDB.V2.Engine.Schema;
using SharpDB.V2.Engine.Vacuum;
using SharpDB.V2.Engine.Backup;
using SharpDB.V2.Infrastructure.IO;

public sealed class Database : IDisposable
{
    private readonly OperationContextFactory _contextFactory = null!;
    private readonly SchemaStore _schemaStore = null!;
    private readonly AutoIncrementStore _autoIncrementStore = null!;
    private readonly TransactionManager _txManager = null!;
    private readonly LogWriter _logWriter = null!;
    private readonly VacuumEngine _vacuumEngine = null!;
    private readonly BackupEngine _backupEngine = null!;

    public Database(DatabaseOptions options) { }

    public Collection<T, TKey> OpenCollection<T, TKey>(string name, CollectionOptions<T, TKey> options)
        => throw new NotImplementedException();

    public void Dispose() { }
}
