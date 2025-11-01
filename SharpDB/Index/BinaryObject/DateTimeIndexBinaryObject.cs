using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

/// <summary>
/// Binary object for DateTime (8 bytes).
/// Stores ticks (long).
/// </summary>
public class DateTimeIndexBinaryObject : AbstractIndexBinaryObject<DateTime>
{
    public const int Bytes = sizeof(long); // 8 bytes (ticks)
    
    public DateTimeIndexBinaryObject(byte[] bytes) : base(bytes)
    {
    }
    
    public DateTimeIndexBinaryObject(DateTime value) : base()
    {
        BitConverter.GetBytes(value.Ticks).CopyTo(_bytes, 0);
    }
    
    public override DateTime AsObject()
    {
        var ticks = BitConverter.ToInt64(_bytes, 0);
        return new DateTime(ticks, DateTimeKind.Utc);
    }
    
    public override int Size => Bytes;
    
    public class Factory : IIndexBinaryObjectFactory<DateTime>
    {
        public IIndexBinaryObject<DateTime> Create(DateTime obj)
        {
            return new DateTimeIndexBinaryObject(obj);
        }
        
        public IIndexBinaryObject<DateTime> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[Bytes];
            Array.Copy(bytes, offset, objBytes, 0, Bytes);
            return new DateTimeIndexBinaryObject(objBytes);
        }
        
        public IIndexBinaryObject<DateTime> CreateEmpty()
        {
            return new DateTimeIndexBinaryObject(DateTime.MinValue);
        }
        
        public int Size => Bytes;
        public Type Type => typeof(DateTime);
    }
}