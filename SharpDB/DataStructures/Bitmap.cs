using System.Text;

namespace SharpDB.DataStructures;

/// <summary>
/// Efficient bit array for tracking null values.
/// </summary>
public class Bitmap(byte[] bytes, int bitCount)
{
    public Bitmap(int bitCount) : this(new byte[(bitCount + 7) / 8], bitCount)
    {
    }

    public int BitCount => bitCount;
    public int ByteSize => bytes.Length;
    
    // Set bit at index
    public void Set(int index, bool value)
    {
        if (index < 0 || index >= bitCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var byteIndex = index / 8;
        var bitIndex = index % 8;
        
        if (value)
        {
            // Set bit to 1
            bytes[byteIndex] |= (byte)(1 << bitIndex);
        }
        else
        {
            // Set bit to 0
            bytes[byteIndex] &= (byte)~(1 << bitIndex);
        }
    }
    
    public bool Get(int index)
    {
        if (index < 0 || index >= bitCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        
        var byteIndex = index / 8;
        var bitIndex = index % 8;
        
        return (bytes[byteIndex] & (1 << bitIndex)) != 0;
    }
    
    public byte[] GetBytes() => bytes;
    
    public int CountSetBits()
    {
        var count = 0;
        for (var i = 0; i < bitCount; i++)
        {
            if (Get(i))
                count++;
        }
        return count;
    }
    
    public void Clear() => Array.Clear(bytes, 0, bytes.Length);
    
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("Bitmap[");
        for (var i = 0; i < Math.Min(bitCount, 64); i++)
        {
            sb.Append(Get(i) ? '1' : '0');
        }
        if (bitCount > 64)
            sb.Append("...");
        sb.Append(']');
        return sb.ToString();
    }
}