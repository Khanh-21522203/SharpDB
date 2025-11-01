namespace SharpDB.Storage.Page;

/// <summary>
///     Fixed-size block of storage (4KB default).
///     Container for multiple DBObjects.
/// </summary>
public class Page : IDisposable
{
    public const int HeaderSize = 8; // PageNumber(4) + UsedSpace(4)

    /// <summary>
    ///     Create new empty page.
    /// </summary>
    public Page(int pageNumber, int pageSize)
    {
        if (pageSize < 512 || pageSize > 65536)
            throw new ArgumentException("Page size must be between 512 and 65536 bytes");

        PageNumber = pageNumber;
        Data = new byte[pageSize];
        UsedSpace = HeaderSize;

        WriteHeader();
    }

    /// <summary>
    ///     Load existing page from bytes.
    /// </summary>
    public Page(byte[] data, int pageNumber)
    {
        Data = data ?? throw new ArgumentNullException(nameof(data));
        PageNumber = pageNumber;
        UsedSpace = BitConverter.ToInt32(data, 4);
    }

    public byte[] Data { get; }

    public int PageNumber { get; }

    public int UsedSpace { get; private set; }

    public int FreeSpace => Data.Length - UsedSpace;
    public bool Modified { get; private set; }

    public void Dispose()
    {
        Array.Clear(Data, 0, Data.Length);
        GC.SuppressFinalize(this);
    }

    private void WriteHeader()
    {
        BitConverter.GetBytes(PageNumber).CopyTo(Data, 0);
        BitConverter.GetBytes(UsedSpace).CopyTo(Data, 4);
    }

    /// <summary>
    ///     Allocate space for new object.
    /// </summary>
    public DBObject? AllocateObject(int schemeId, int collectionId, int version, byte[] objectData)
    {
        var requiredSize = DBObject.MetaBytes + objectData.Length;

        if (FreeSpace < requiredSize)
            return null; // Not enough space

        var objectBegin = UsedSpace;
        var objectEnd = objectBegin + requiredSize;

        // Write metadata
        Data[objectBegin] = DBObject.AliveFlag;
        BitConverter.GetBytes(schemeId).CopyTo(Data, objectBegin + 1);
        BitConverter.GetBytes(collectionId).CopyTo(Data, objectBegin + 5);
        BitConverter.GetBytes(version).CopyTo(Data, objectBegin + 9);
        BitConverter.GetBytes(objectData.Length).CopyTo(Data, objectBegin + 13);

        // Write data
        Array.Copy(objectData, 0, Data, objectBegin + DBObject.MetaBytes, objectData.Length);

        UsedSpace = objectEnd;
        WriteHeader();
        MarkModified();

        return new DBObject(this, objectBegin, objectEnd);
    }

    /// <summary>
    ///     Get object at specific offset.
    /// </summary>
    public DBObject GetObjectAt(int offset)
    {
        if (offset < HeaderSize || offset >= UsedSpace)
            throw new ArgumentOutOfRangeException(nameof(offset));

        var dataSize = BitConverter.ToInt32(Data, offset + 13);
        var objectEnd = offset + DBObject.MetaBytes + dataSize;

        return new DBObject(this, offset, objectEnd);
    }

    /// <summary>
    ///     Enumerate all objects in page.
    /// </summary>
    public IEnumerable<DBObject> GetObjects()
    {
        var cursor = HeaderSize;

        while (cursor < UsedSpace)
        {
            var dataSize = BitConverter.ToInt32(Data, cursor + 13);
            var objectEnd = cursor + DBObject.MetaBytes + dataSize;

            yield return new DBObject(this, cursor, objectEnd);

            cursor = objectEnd;
        }
    }

    public void MarkModified()
    {
        Modified = true;
    }

    public void ClearModified()
    {
        Modified = false;
    }
}