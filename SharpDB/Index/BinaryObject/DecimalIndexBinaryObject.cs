using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

/// <summary>
///     Binary object for decimal (16 bytes).
///     Uses decimal.GetBits for serialization.
/// </summary>
public class DecimalIndexBinaryObject : AbstractIndexBinaryObject<decimal>
{
    public const int Bytes = sizeof(decimal); // 16

    public DecimalIndexBinaryObject(byte[] bytes) : base(bytes)
    {
    }

    public DecimalIndexBinaryObject(decimal value)
    {
        var bits = decimal.GetBits(value);

        for (var i = 0; i < 4; i++) BitConverter.GetBytes(bits[i]).CopyTo(_bytes, i * 4);
    }

    public override int Size => Bytes;

    public override decimal AsObject()
    {
        var bits = new int[4];

        for (var i = 0; i < 4; i++) bits[i] = BitConverter.ToInt32(_bytes, i * 4);

        return new decimal(bits);
    }

    public class Factory : IIndexBinaryObjectFactory<decimal>
    {
        public IIndexBinaryObject<decimal> Create(decimal obj)
        {
            return new DecimalIndexBinaryObject(obj);
        }

        public IIndexBinaryObject<decimal> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[Bytes];
            Array.Copy(bytes, offset, objBytes, 0, Bytes);
            return new DecimalIndexBinaryObject(objBytes);
        }

        public IIndexBinaryObject<decimal> CreateEmpty()
        {
            return new DecimalIndexBinaryObject(0m);
        }

        public int Size => Bytes;
        public Type Type => typeof(decimal);
    }
}