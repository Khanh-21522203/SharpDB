namespace SharpDB.Utils;

public static class BinaryUtils
{
    public static bool IsAllZeros(byte[] data, int offset, int length)
    {
        for (var i = offset; i < offset + length; i++)
        {
            if (i >= data.Length)
                return true;
            
            if (data[i] != 0)
                return false;
        }
        return true;
    }
}