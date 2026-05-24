namespace SharpDB.V2.Engine.Types;

public readonly record struct LogSequenceNumber(long Value) : IComparable<LogSequenceNumber>
{
    public int CompareTo(LogSequenceNumber other) => Value.CompareTo(other.Value);
}
