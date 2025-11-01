using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

/// <summary>
///     Binary object for long (8 bytes).
///     Most common type for primary keys.
/// </summary>
public class LongIndexBinaryObject : AbstractIndexBinaryObject<long>
{
    public const int Bytes = sizeof(long); // 8

    public LongIndexBinaryObject(byte[] bytes) : base(bytes)
    {
    }

    public LongIndexBinaryObject(long value)
    {
        BitConverter.GetBytes(value).CopyTo(_bytes, 0);
    }

    public override int Size => Bytes;

    public override long AsObject()
    {
        return BitConverter.ToInt64(_bytes, 0);
    }

    public class Factory : IIndexBinaryObjectFactory<long>
    {
        public IIndexBinaryObject<long> Create(long obj)
        {
            return new LongIndexBinaryObject(obj);
        }

        public IIndexBinaryObject<long> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[Bytes];
            Array.Copy(bytes, offset, objBytes, 0, Bytes);
            return new LongIndexBinaryObject(objBytes);
        }

        public IIndexBinaryObject<long> CreateEmpty()
        {
            return new LongIndexBinaryObject(0L);
        }

        public int Size => Bytes;
        public Type Type => typeof(long);
    }
}