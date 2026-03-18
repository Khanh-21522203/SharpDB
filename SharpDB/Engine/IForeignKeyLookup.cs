namespace SharpDB.Engine;

public interface IForeignKeyLookup
{
    string CollectionName { get; }
    string PrimaryKeyFieldName { get; }
    Task<bool> ExistsByPrimaryKeyAsync(object key);
}
