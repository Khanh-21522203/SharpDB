using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Abstractions.Backup;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Backup;

public sealed class BackupEngine : IBackupEngine
{
    public BackupManifest Create(string destinationPath, IOperationContext ctx) => throw new NotImplementedException();
}
