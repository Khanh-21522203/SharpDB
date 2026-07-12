using SharpDB.V2.Engine.Partitioning.Strategies;

namespace SharpDB.V2.Tests.Unit.Partitioning;

using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using Xunit;

public sealed class PartitionRouterTests
{
    [Fact]
    public void Route_ReturnsThePartition_SelectedByTheStrategy()
    {
        var partitions = new[]
        {
            new DataPartitionDescriptor { Id = 0, Name = "p0" },
            new DataPartitionDescriptor { Id = 1, Name = "p1" },
            new DataPartitionDescriptor { Id = 2, Name = "p2" },
        };
        var router = new PartitionRouter<int>(new HashPartitionStrategy<int>(), partitions);

        var expectedIndex = new HashPartitionStrategy<int>().GetPartition(7, partitions.Length);

        router.Route(7).Should().BeSameAs(partitions[expectedIndex]);
    }

    [Fact]
    public void Route_IsConsistent_ForTheSameKey()
    {
        var partitions = new[]
        {
            new DataPartitionDescriptor { Id = 0, Name = "p0" },
            new DataPartitionDescriptor { Id = 1, Name = "p1" },
        };
        var router = new PartitionRouter<string>(new HashPartitionStrategy<string>(), partitions);

        router.Route("customer-42").Should().BeSameAs(router.Route("customer-42"));
    }

    [Fact]
    public void RouteRange_ReturnsOnlyThePartitions_SelectedByTheStrategy()
    {
        var partitions = new[]
        {
            new DataPartitionDescriptor { Id = 0, Name = "p0" },
            new DataPartitionDescriptor { Id = 1, Name = "p1" },
            new DataPartitionDescriptor { Id = 2, Name = "p2" },
        };
        var router = new PartitionRouter<int>(new RangePartitionStrategy<int>([10, 20]), partitions);

        router.RouteRange(12, 18).Should().Equal(partitions[1]);
    }
}
