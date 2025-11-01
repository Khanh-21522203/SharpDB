namespace SharpDB.DataStructures;

/// <summary>
///     Efficient bit array for tracking null values.
/// </summary>
public class Bitmap(int capacity)
{
    private readonly byte[] _bits = new byte[(capacity + 7) / 8];
    public int Capacity { get; } = capacity;

    public void Set(int index)
    {
        if (index < 0 || index >= Capacity)
            throw new ArgumentOutOfRangeException(nameof(index));

        _bits[index / 8] |= (byte)(1 << (index % 8));
    }

    public void Clear(int index)
    {
        if (index < 0 || index >= Capacity)
            throw new ArgumentOutOfRangeException(nameof(index));

        _bits[index / 8] &= (byte)~(1 << (index % 8));
    }

    public bool IsSet(int index)
    {
        if (index < 0 || index >= Capacity)
            return false;

        return (_bits[index / 8] & (1 << (index % 8))) != 0;
    }
}