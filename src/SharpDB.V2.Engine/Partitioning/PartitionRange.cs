namespace SharpDB.V2.Engine.Partitioning;

public readonly record struct PartitionRange<TKey>(
    bool HasLowerBound,
    TKey LowerBound,
    bool HasUpperBound,
    TKey UpperBound)
{
    public static PartitionRange<TKey> All => default;

    public static PartitionRange<TKey> From(TKey lowerBound) =>
        new(true, lowerBound, false, default!);

    public static PartitionRange<TKey> To(TKey upperBound) =>
        new(false, default!, true, upperBound);

    public static PartitionRange<TKey> ClosedOpen(TKey lowerBound, TKey upperBound) =>
        new(true, lowerBound, true, upperBound);
}
