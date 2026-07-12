namespace SharpDB.V2.Tests.Unit;

using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using Xunit;

public sealed class CollectionOptionsTests
{
    [Fact]
    public void Defaults_ToUnpartitionedSinglePartition()
    {
        var options = new CollectionOptions<object, int>();

        options.PartitionKind.Should().Be(PartitionKind.None);
        options.PartitionCount.Should().Be(1);
        options.PartitionStrategy.Should().BeNull();
    }
}
