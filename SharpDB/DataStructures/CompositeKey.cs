using System.Collections;

namespace SharpDB.DataStructures;

/// <summary>
/// A composite key that combines multiple field values into a single comparable byte array.
/// Supports lexicographic comparison for B+Tree ordering.
/// </summary>
public readonly struct CompositeKey : IComparable<CompositeKey>, IEquatable<CompositeKey>
{
    internal readonly byte[] Data;

    public CompositeKey(byte[] data) => Data = data ?? [];

    public int CompareTo(CompositeKey other)
    {
        var a = Data;
        var b = other.Data;
        var len = Math.Min(a.Length, b.Length);
        for (var i = 0; i < len; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }
        return a.Length.CompareTo(b.Length);
    }

    public bool Equals(CompositeKey other) => CompareTo(other) == 0;
    public override bool Equals(object? obj) => obj is CompositeKey other && Equals(other);
    public override int GetHashCode() => StructuralComparisons.StructuralEqualityComparer.GetHashCode(Data);
    public static bool operator ==(CompositeKey left, CompositeKey right) => left.Equals(right);
    public static bool operator !=(CompositeKey left, CompositeKey right) => !left.Equals(right);
}
