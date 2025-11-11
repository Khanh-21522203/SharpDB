namespace SharpDB.Engine;

public class CollectionInfo
{
    public int CollectionId { get; set; }
    public string Name { get; set; } = "";
    public int SchemaVersion { get; set; }
    public long RecordCount { get; set; }
    public DateTime CreatedAt { get; set; }
}