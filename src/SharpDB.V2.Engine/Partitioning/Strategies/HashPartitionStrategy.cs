using System.Buffers.Binary;
using System.Text;
using SharpDB.V2.Engine.Abstractions.Partitioning;

namespace SharpDB.V2.Engine.Partitioning.Strategies;

/// <summary>
/// Hashes over each key's bytes with FNV-1a rather than <see cref="object.GetHashCode"/>. .NET randomizes
/// <see cref="string.GetHashCode()"/> per process, and <see cref="Array"/> doesn't override
/// <see cref="object.GetHashCode"/> at all (reference identity), so neither is stable across a restart or
/// even within a single run. This matters because partition assignment is persisted (see
/// <see cref="Schema.CollectionSchema.Partitions"/>) and must resolve to the same partition every time.
/// </summary>
public sealed class HashPartitionStrategy<TKey> : IPartitionStrategy<TKey>
{
    private readonly Func<TKey, byte[]> _keyToBytes;

    public HashPartitionStrategy(Func<TKey, byte[]>? keyToBytes = null)
    {
        _keyToBytes = keyToBytes ?? DefaultKeyToBytes;
    }

    public int GetPartition(TKey key, IReadOnlyList<DataPartitionDescriptor> partitions) =>
        GetPartition(key, ValidatePartitionCount(partitions));

    public int GetPartition(TKey key, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount));
        }

        if (key is null)
        {
            return 0;
        }

        return (int)(Fnv1A(_keyToBytes(key)) % (uint)partitionCount);
    }

    public IReadOnlyList<int> GetPartitionsForRange(
        PartitionRange<TKey> range,
        IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        var partitionCount = ValidatePartitionCount(partitions);
        var result = new int[partitionCount];
        for (var i = 0; i < result.Length; i++)
        {
            result[i] = i;
        }

        return result;
    }

    private static byte[] DefaultKeyToBytes(TKey key) => key switch
    {
        string s => Encoding.UTF8.GetBytes(s),
        int i => WriteInt32(i),
        long l => WriteInt64(l),
        Guid g => g.ToByteArray(),
        byte[] b => b,
        _ => throw new NotSupportedException(
            $"HashPartitionStrategy<{typeof(TKey).Name}> has no built-in byte conversion for key type " +
            $"'{key!.GetType().Name}'. Supported out of the box: string, int, long, Guid, byte[]. Pass a " +
            "keyToBytes delegate to the constructor for other key types."),
    };

    private static byte[] WriteInt32(int value)
    {
        var bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return bytes;
    }

    private static byte[] WriteInt64(long value)
    {
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }

    private static int ValidatePartitionCount(IReadOnlyList<DataPartitionDescriptor> partitions)
    {
        if (partitions.Count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitions));
        }

        return partitions.Count;
    }

    private static uint Fnv1A(ReadOnlySpan<byte> data)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;

        var hash = offsetBasis;
        foreach (var b in data)
        {
            hash ^= b;
            hash *= prime;
        }

        return hash;
    }
}
