using SharpDB.V2.Engine.Types;

namespace SharpDB.V2.Engine.Abstractions.Storage;

public interface IPage
{
    PageId Id { get; }
    ReadOnlySpan<byte> Data { get; }
    Span<byte> GetWritableData();
    bool IsDirty { get; }
    void MarkDirty();
}
