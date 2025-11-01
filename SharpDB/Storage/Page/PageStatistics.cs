namespace SharpDB.Storage.Page;

public class PageStatistics
{
    public int CollectionId { get; set; }
    public int TotalPages { get; set; }
    public int FreePages { get; set; }
    public int UsedPages { get; set; }
    public long FileSizeBytes { get; set; }
    
    public double FreePagePercentage => TotalPages > 0 
        ? (FreePages * 100.0 / TotalPages) 
        : 0;
}