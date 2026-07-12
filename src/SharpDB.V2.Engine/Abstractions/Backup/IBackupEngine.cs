using SharpDB.V2.Engine.Abstractions;
using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Backup;

public interface IBackupEngine
{
    BackupManifest Create(string destinationPath, IOperationContext ctx);
}
