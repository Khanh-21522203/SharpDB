using SharpDB.V2.Engine.Partitioning.Strategies;

namespace SharpDB.V2.Tests.Unit.Partitioning;

using System;
using System.Text;
using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using Xunit;

public sealed class HashPartitionStrategyTests
{
    [Fact]
    public void GetPartition_IsWithinRange_ForStringKeys()
    {
        var strategy = new HashPartitionStrategy<string>();

        foreach (var key in new[] { "a", "customer-42", "", "some much longer key with spaces" })
        {
            strategy.GetPartition(key, partitionCount: 4).Should().BeInRange(0, 3);
        }
    }

    [Fact]
    public void GetPartition_IsWithinRange_ForIntLongGuidAndByteArrayKeys()
    {
        new HashPartitionStrategy<int>().GetPartition(-7, 4).Should().BeInRange(0, 3);
        new HashPartitionStrategy<long>().GetPartition(long.MinValue, 4).Should().BeInRange(0, 3);
        new HashPartitionStrategy<Guid>().GetPartition(Guid.NewGuid(), 4).Should().BeInRange(0, 3);
        new HashPartitionStrategy<byte[]>().GetPartition([1, 2, 3], 4).Should().BeInRange(0, 3);
    }

    [Fact]
    public void GetPartition_IsDeterministic_AcrossFreshInstances()
    {
        // Simulates "across a process restart": a brand-new strategy instance still resolves the same
        // key to the same partition. This is exactly the property object.GetHashCode() does not guarantee
        // for string (randomized per process) or byte[] (reference identity).
        new HashPartitionStrategy<string>().GetPartition("customer-42", 8)
            .Should().Be(new HashPartitionStrategy<string>().GetPartition("customer-42", 8));
    }

    [Fact]
    public void GetPartition_IsDeterministic_ForDifferentByteArraysWithSameContents()
    {
        var strategy = new HashPartitionStrategy<byte[]>();

        strategy.GetPartition([1, 2, 3], 8).Should().Be(strategy.GetPartition([1, 2, 3], 8));
    }

    [Fact]
    public void GetPartition_Throws_WhenPartitionCountIsNotPositive()
    {
        var strategy = new HashPartitionStrategy<string>();

        var act = () => strategy.GetPartition("k", partitionCount: 0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void GetPartition_ReturnsZero_ForNullKey()
    {
        var strategy = new HashPartitionStrategy<string>();

        strategy.GetPartition(null!, partitionCount: 4).Should().Be(0);
    }

    [Fact]
    public void GetPartition_Throws_ForUnsupportedKeyType_WithoutACustomConverter()
    {
        var strategy = new HashPartitionStrategy<Uri>();

        var act = () => strategy.GetPartition(new Uri("https://example.com"), partitionCount: 4);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void GetPartition_UsesTheSuppliedConverter_ForUnsupportedKeyTypes()
    {
        var strategy = new HashPartitionStrategy<Uri>(uri => Encoding.UTF8.GetBytes(uri.ToString()));

        strategy.GetPartition(new Uri("https://example.com"), partitionCount: 4).Should().BeInRange(0, 3);
    }

    [Fact]
    public void GetPartitionsForRange_ReturnsAllPartitions()
    {
        var strategy = new HashPartitionStrategy<int>();
        var partitions = new[]
        {
            new DataPartitionDescriptor { Id = 0, Name = "p0" },
            new DataPartitionDescriptor { Id = 1, Name = "p1" },
            new DataPartitionDescriptor { Id = 2, Name = "p2" },
        };

        strategy.GetPartitionsForRange(PartitionRange<int>.ClosedOpen(10, 20), partitions).Should().Equal(0, 1, 2);
    }
}
