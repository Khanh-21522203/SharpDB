using System.Text;
using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

/// <summary>
///     Binary object for fixed-length strings.
///     Strings are padded/truncated to maxSize.
///     UTF-8 encoded.
/// </summary>
public class StringIndexBinaryObject : AbstractIndexBinaryObject<string>
{
    private readonly int _maxSize;

    public StringIndexBinaryObject(byte[] bytes, int maxSize) : base(bytes)
    {
        _maxSize = maxSize;
    }

    public StringIndexBinaryObject(string value, int maxSize)
    {
        _maxSize = maxSize;
        _bytes = new byte[maxSize];

        var encoded = Encoding.UTF8.GetBytes(value);
        var length = Math.Min(encoded.Length, maxSize);
        Array.Copy(encoded, 0, _bytes, 0, length);
    }

    public override int Size => _maxSize;

    public override string AsObject()
    {
        return Encoding.UTF8.GetString(_bytes).TrimEnd('\0');
    }

    public class Factory(int maxSize) : IIndexBinaryObjectFactory<string>
    {
        public IIndexBinaryObject<string> Create(string obj)
        {
            return new StringIndexBinaryObject(obj, maxSize);
        }

        public IIndexBinaryObject<string> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[maxSize];
            Array.Copy(bytes, offset, objBytes, 0, maxSize);
            return new StringIndexBinaryObject(objBytes, maxSize);
        }

        public IIndexBinaryObject<string> CreateEmpty()
        {
            return new StringIndexBinaryObject(string.Empty, maxSize);
        }

        public int Size => maxSize;
        public Type Type => typeof(string);
    }
}