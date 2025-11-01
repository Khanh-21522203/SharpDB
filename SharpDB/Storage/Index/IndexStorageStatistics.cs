namespace SharpDB.Storage.Index;

/// <summary>
///     Statistics for index storage monitoring.
/// </summary>
public class IndexStorageStatistics
{
    public int IndexId { get; set; }
    public long TotalNodes { get; set; }
    public long FreePositions { get; set; }
    public long FileSizeBytes { get; set; }
    public DateTime LastAccess { get; set; }

    public double FragmentationPercentage => TotalNodes > 0
        ? FreePositions * 100.0 / TotalNodes
        : 0;
}

/// <summary>
///     Extension for storage monitoring.
/// </summary>
public static class IndexStorageExtensions
{
    public static IndexStorageStatistics GetStatisticsAsync(
        this DiskPageFileIndexStorageManager manager,
        int indexId)
    {
        // Implementation would track metrics
        return new IndexStorageStatistics
        {
            IndexId = indexId,
            LastAccess = DateTime.UtcNow
        };
    }
}