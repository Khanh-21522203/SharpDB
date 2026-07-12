using SharpDB.V2.Engine.Partitioning.Strategies;

namespace SharpDB.V2.Tests.Unit.Partitioning;

using System;
using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using Xunit;

public sealed class SinglePartitionStrategyTests
{
    [Fact]
    public void GetPartition_AlwaysReturnsTheOnlyPartition()
    {
        var strategy = new SinglePartitionStrategy<int>();
        var partitions = new[] { new DataPartitionDescriptor { Id = 0, Name = "single" } };

        strategy.GetPartition(1, partitions).Should().Be(0);
        strategy.GetPartition(999, partitions).Should().Be(0);
    }

    [Fact]
    public void GetPartitionsForRange_AlwaysReturnsTheOnlyPartition()
    {
        var strategy = new SinglePartitionStrategy<int>();
        var partitions = new[] { new DataPartitionDescriptor { Id = 0, Name = "single" } };

        strategy.GetPartitionsForRange(PartitionRange<int>.ClosedOpen(10, 20), partitions).Should().Equal(0);
    }

    [Fact]
    public void GetPartition_Throws_WhenMoreThanOnePartitionIsConfigured()
    {
        var strategy = new SinglePartitionStrategy<int>();
        var partitions = new[]
        {
            new DataPartitionDescriptor { Id = 0, Name = "p0" },
            new DataPartitionDescriptor { Id = 1, Name = "p1" },
        };

        var act = () => strategy.GetPartition(1, partitions);

        act.Should().Throw<ArgumentException>();
    }
}
