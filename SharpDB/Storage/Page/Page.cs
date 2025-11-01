namespace SharpDB.Storage.Page;

/// <summary>
/// Fixed-size block of storage (4KB default).
/// Container for multiple DBObjects.
/// </summary>
public class Page : IDisposable
{
    public const int HeaderSize = 8; // PageNumber(4) + UsedSpace(4)
    
    private readonly byte[] _data;
    private readonly int _pageNumber;
    private int _usedSpace;
    private bool _modified;
    
    public byte[] Data => _data;
    public int PageNumber => _pageNumber;
    public int UsedSpace => _usedSpace;
    public int FreeSpace => _data.Length - _usedSpace;
    public bool Modified => _modified;
    
    /// <summary>
    /// Create new empty page.
    /// </summary>
    public Page(int pageNumber, int pageSize)
    {
        if (pageSize < 512 || pageSize > 65536)
            throw new ArgumentException("Page size must be between 512 and 65536 bytes");
            
        _pageNumber = pageNumber;
        _data = new byte[pageSize];
        _usedSpace = HeaderSize;
        
        WriteHeader();
    }
    
    /// <summary>
    /// Load existing page from bytes.
    /// </summary>
    public Page(byte[] data, int pageNumber)
    {
        _data = data ?? throw new ArgumentNullException(nameof(data));
        _pageNumber = pageNumber;
        _usedSpace = BitConverter.ToInt32(data, 4);
    }
    
    private void WriteHeader()
    {
        BitConverter.GetBytes(_pageNumber).CopyTo(_data, 0);
        BitConverter.GetBytes(_usedSpace).CopyTo(_data, 4);
    }
    
    /// <summary>
    /// Allocate space for new object.
    /// </summary>
    public DBObject? AllocateObject(int schemeId, int collectionId, int version, byte[] objectData)
    {
        int requiredSize = DBObject.MetaBytes + objectData.Length;
        
        if (FreeSpace < requiredSize)
            return null; // Not enough space
        
        int objectBegin = _usedSpace;
        int objectEnd = objectBegin + requiredSize;
        
        // Write metadata
        _data[objectBegin] = DBObject.AliveFlag;
        BitConverter.GetBytes(schemeId).CopyTo(_data, objectBegin + 1);
        BitConverter.GetBytes(collectionId).CopyTo(_data, objectBegin + 5);
        BitConverter.GetBytes(version).CopyTo(_data, objectBegin + 9);
        BitConverter.GetBytes(objectData.Length).CopyTo(_data, objectBegin + 13);
        
        // Write data
        Array.Copy(objectData, 0, _data, objectBegin + DBObject.MetaBytes, objectData.Length);
        
        _usedSpace = objectEnd;
        WriteHeader();
        MarkModified();
        
        return new DBObject(this, objectBegin, objectEnd);
    }
    
    /// <summary>
    /// Get object at specific offset.
    /// </summary>
    public DBObject GetObjectAt(int offset)
    {
        if (offset < HeaderSize || offset >= _usedSpace)
            throw new ArgumentOutOfRangeException(nameof(offset));
        
        int dataSize = BitConverter.ToInt32(_data, offset + 13);
        int objectEnd = offset + DBObject.MetaBytes + dataSize;
        
        return new DBObject(this, offset, objectEnd);
    }
    
    /// <summary>
    /// Enumerate all objects in page.
    /// </summary>
    public IEnumerable<DBObject> GetObjects()
    {
        int cursor = HeaderSize;
        
        while (cursor < _usedSpace)
        {
            int dataSize = BitConverter.ToInt32(_data, cursor + 13);
            int objectEnd = cursor + DBObject.MetaBytes + dataSize;
            
            yield return new DBObject(this, cursor, objectEnd);
            
            cursor = objectEnd;
        }
    }
    
    public void MarkModified() => _modified = true;
    public void ClearModified() => _modified = false;
    
    public void Dispose()
    {
        Array.Clear(_data, 0, _data.Length);
        GC.SuppressFinalize(this);
    }
}
