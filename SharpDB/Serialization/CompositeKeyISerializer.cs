using SharpDB.Core.Abstractions.Serialization;
using SharpDB.DataStructures;

namespace SharpDB.Serialization;

/// <summary>
/// Wraps CompositeKeySerializer to implement ISerializer&lt;CompositeKey&gt; for B+Tree usage.
/// </summary>
public sealed class CompositeKeyISerializer : ISerializer<CompositeKey>
{
    private readonly CompositeKeySerializer _inner;
    public int Size => _inner.TotalSize;

    public CompositeKeyISerializer(CompositeKeySerializer inner) => _inner = inner;

    public byte[] Serialize(CompositeKey value)
    {
        var data = value.Data ?? [];
        if (data.Length == _inner.TotalSize)
            return data;

        var result = new byte[_inner.TotalSize];
        var copyLen = Math.Min(data.Length, _inner.TotalSize);
        Array.Copy(data, 0, result, 0, copyLen);
        return result;
    }

    public void SerializeTo(CompositeKey value, Span<byte> dest)
    {
        var data = value.Data ?? [];
        var copyLen = Math.Min(data.Length, _inner.TotalSize);
        data.AsSpan(0, copyLen).CopyTo(dest);
        if (copyLen < _inner.TotalSize)
            dest.Slice(copyLen, _inner.TotalSize - copyLen).Clear();
    }

    public CompositeKey Deserialize(byte[] buffer, int offset = 0)
    {
        var data = new byte[_inner.TotalSize];
        Array.Copy(buffer, offset, data, 0, _inner.TotalSize);
        return new CompositeKey(data);
    }
}
