using SharpDB.V2.Engine.Partitioning.Strategies;

namespace SharpDB.V2.Tests.Unit.Partitioning;

using System;
using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using Xunit;

public sealed class RangePartitionStrategyTests
{
    private static readonly DataPartitionDescriptor[] Partitions =
    [
        new() { Id = 0, Name = "p0" },
        new() { Id = 1, Name = "p1" },
        new() { Id = 2, Name = "p2" },
    ];

    [Theory]
    [InlineData(0, 0)]
    [InlineData(9, 0)]
    [InlineData(10, 1)]
    [InlineData(19, 1)]
    [InlineData(20, 2)]
    [InlineData(100, 2)]
    public void GetPartition_RoutesUsingLowerInclusiveUpperExclusiveRanges(int key, int expectedPartition)
    {
        var strategy = new RangePartitionStrategy<int>([10, 20]);

        strategy.GetPartition(key, Partitions).Should().Be(expectedPartition);
    }

    [Theory]
    [InlineData(1, 9, new[] { 0 })]
    [InlineData(1, 10, new[] { 0 })]
    [InlineData(1, 11, new[] { 0, 1 })]
    [InlineData(10, 20, new[] { 1 })]
    [InlineData(10, 21, new[] { 1, 2 })]
    [InlineData(19, 100, new[] { 1, 2 })]
    public void GetPartitionsForRange_ReturnsOverlappingPartitionsOnly(int from, int to, int[] expectedPartitions)
    {
        var strategy = new RangePartitionStrategy<int>([10, 20]);

        strategy.GetPartitionsForRange(PartitionRange<int>.ClosedOpen(from, to), Partitions).Should().Equal(expectedPartitions);
    }

    [Fact]
    public void GetPartitionsForRange_SupportsOpenBounds()
    {
        var strategy = new RangePartitionStrategy<int>([10, 20]);

        strategy.GetPartitionsForRange(PartitionRange<int>.To(10), Partitions).Should().Equal(0);
        strategy.GetPartitionsForRange(PartitionRange<int>.From(20), Partitions).Should().Equal(2);
        strategy.GetPartitionsForRange(PartitionRange<int>.All, Partitions).Should().Equal(0, 1, 2);
    }

    [Fact]
    public void GetPartitionsForRange_ReturnsEmpty_WhenRangeStartsAndEndsAtSameBoundary()
    {
        var strategy = new RangePartitionStrategy<int>([10, 20]);

        strategy.GetPartitionsForRange(PartitionRange<int>.ClosedOpen(10, 10), Partitions).Should().BeEmpty();
        strategy.GetPartitionsForRange(PartitionRange<int>.ClosedOpen(11, 11), Partitions).Should().BeEmpty();
    }

    [Fact]
    public void Constructor_Throws_WhenUpperBoundsAreNotStrictlyIncreasing()
    {
        var act = () => new RangePartitionStrategy<int>([10, 10]);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void GetPartition_Throws_WhenPartitionCountDoesNotMatchBounds()
    {
        var strategy = new RangePartitionStrategy<int>([10, 20]);

        var act = () => strategy.GetPartition(1, Partitions[..2]);

        act.Should().Throw<ArgumentException>();
    }
}
