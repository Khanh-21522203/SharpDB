namespace SharpDB.Storage.Page;

/// <summary>
///     Wrapper for data object within a page.
///     Zero-copy reference to page data.
/// </summary>
public class DBObject
{
    public const byte AliveFlag = 0x01;
    public const int MetaBytes = 17; // Flag(1) + SchemeId(4) + CollectionId(4) + Version(4) + DataSize(4)

    private readonly byte[] _wrappedData;

    public DBObject(Page page, int begin, int end)
    {
        Page = page ?? throw new ArgumentNullException(nameof(page));
        _wrappedData = page.Data;
        Begin = begin;
        End = end;

        Verify();
    }

    public int Begin { get; }

    public int End { get; }

    public int Length => End - Begin;
    public Page Page { get; }
    public bool Modified { get; private set; }

    // Metadata accessors
    public int SchemeId => BitConverter.ToInt32(_wrappedData, Begin + 1);
    public int CollectionId => BitConverter.ToInt32(_wrappedData, Begin + 5);
    public int Version => BitConverter.ToInt32(_wrappedData, Begin + 9);
    public int DataSize => BitConverter.ToInt32(_wrappedData, Begin + 13);
    public bool IsAlive => (_wrappedData[Begin] & AliveFlag) == AliveFlag;

    /// <summary>
    ///     Get data portion (without metadata).
    /// </summary>
    public byte[] Data
    {
        get
        {
            var result = new byte[DataSize];
            Array.Copy(_wrappedData, Begin + MetaBytes, result, 0, DataSize);
            return result;
        }
    }

    private void Verify()
    {
        if (End > _wrappedData.Length)
            throw new InvalidOperationException($"End {End} exceeds data length {_wrappedData.Length}");

        if (Length < MetaBytes + 1)
            throw new InvalidOperationException($"Minimum size is {MetaBytes + 1}, got {Length}");
    }

    /// <summary>
    ///     Modify data in-place.
    /// </summary>
    public void ModifyData(byte[] value)
    {
        if (value.Length > DataSize)
            throw new InvalidOperationException("Cannot extend DBObject size. Create a new one.");

        SetSize(value.Length);
        Array.Copy(value, 0, _wrappedData, Begin + MetaBytes, value.Length);
        Modified = true;
        Page.MarkModified();
    }

    /// <summary>
    ///     Mark as deleted (soft delete).
    /// </summary>
    public void MarkDeleted()
    {
        _wrappedData[Begin] &= unchecked((byte)~AliveFlag);
        Modified = true;
        Page.MarkModified();
    }

    private void SetSize(int size)
    {
        BitConverter.GetBytes(size).CopyTo(_wrappedData, Begin + 13);
    }
}