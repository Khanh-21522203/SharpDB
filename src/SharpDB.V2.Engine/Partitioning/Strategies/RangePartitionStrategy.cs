using SharpDB.V2.Engine.Abstractions.Partitioning;

namespace SharpDB.V2.Engine.Partitioning.Strategies;

/// <summary>
/// Routes keys through sorted upper-bound boundaries. Partition ranges are lower-inclusive and
/// upper-exclusive: (-inf, b0), [b0, b1), ... [last, +inf).
/// </summary>
public sealed class RangePartitionStrategy<TKey> : IPartitionStrategy<TKey>
{
    private readonly TKey[] _upperBounds;
    private readonly IComparer<TKey> _comparer;

    public RangePartitionStrategy(IEnumerable<TKey> upperBounds, IComparer<TKey>? comparer = null)
    {
        _comparer = comparer ?? Comparer<TKey>.Default;
        _upperBounds = upperBounds.ToArray();
        ValidateSortedBounds(_upperBounds, _comparer);
    }

    public int GetPartition(TKey key, IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        ValidatePartitionShape(partitions);
        return GetPartitionIndex(key);
    }

    public IReadOnlyList<int> GetPartitionsForRange(
        PartitionRange<TKey> range,
        IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        ValidatePartitionShape(partitions);

        if (range is { HasLowerBound: true, HasUpperBound: true } &&
            _comparer.Compare(range.LowerBound, range.UpperBound) > 0)
        {
            throw new ArgumentException("Range lower bound must be less than or equal to upper bound.", nameof(range));
        }

        if (range is { HasLowerBound: true, HasUpperBound: true } &&
            _comparer.Compare(range.LowerBound, range.UpperBound) == 0)
        {
            return [];
        }

        var first = range.HasLowerBound ? GetPartitionIndex(range.LowerBound) : 0;
        var last = range.HasUpperBound
            ? GetPartitionIndexForExclusiveUpperBound(range.UpperBound)
            : partitions.Count - 1;
        if (last < first)
        {
            return [];
        }

        var result = new int[last - first + 1];
        for (var i = 0; i < result.Length; i++)
        {
            result[i] = first + i;
        }

        return result;
    }

    private int GetPartitionIndex(TKey key)
    {
        var low = 0;
        var high = _upperBounds.Length;
        while (low < high)
        {
            var mid = low + ((high - low) / 2);
            if (_comparer.Compare(key, _upperBounds[mid]) < 0)
            {
                high = mid;
            }
            else
            {
                low = mid + 1;
            }
        }

        return low;
    }

    private int GetPartitionIndexForExclusiveUpperBound(TKey upperBound)
    {
        var partition = GetPartitionIndex(upperBound);
        return IsPartitionLowerBoundary(upperBound, partition) ? partition - 1 : partition;
    }

    private bool IsPartitionLowerBoundary(TKey key, int partition)
    {
        return partition > 0 && _comparer.Compare(key, _upperBounds[partition - 1]) == 0;
    }

    private void ValidatePartitionShape(IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        if (partitions.Count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitions));
        }

        if (partitions.Count != _upperBounds.Length + 1)
        {
            throw new ArgumentException(
                "Range partition count must equal upper boundary count plus one.",
                nameof(partitions));
        }
    }

    private static void ValidateSortedBounds(TKey[] upperBounds, IComparer<TKey> comparer)
    {
        for (var i = 1; i < upperBounds.Length; i++)
        {
            if (comparer.Compare(upperBounds[i - 1], upperBounds[i]) >= 0)
            {
                throw new ArgumentException("Range partition upper bounds must be strictly increasing.", nameof(upperBounds));
            }
        }
    }
}
