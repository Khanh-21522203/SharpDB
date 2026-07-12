namespace SharpDB.V2.Tests.Unit.Schema;

using FluentAssertions;
using SharpDB.V2.Engine.Schema;
using Xunit;

public sealed class AutoIncrementStoreTests
{
    [Fact]
    public void NextValue_StartsAtOne_ForUnseenCollection()
    {
        var store = new AutoIncrementStore();

        store.NextValue("orders").Should().Be(1);
    }

    [Fact]
    public void NextValue_IncrementsSequentially()
    {
        var store = new AutoIncrementStore();

        store.NextValue("orders").Should().Be(1);
        store.NextValue("orders").Should().Be(2);
        store.NextValue("orders").Should().Be(3);
    }

    [Fact]
    public void Peek_DoesNotAdvanceTheCounter()
    {
        var store = new AutoIncrementStore();
        store.NextValue("orders");

        store.Peek("orders").Should().Be(1);
        store.Peek("orders").Should().Be(1);
        store.NextValue("orders").Should().Be(2);
    }

    [Fact]
    public void Peek_ReturnsZero_ForUnseenCollection()
    {
        var store = new AutoIncrementStore();

        store.Peek("never-touched").Should().Be(0);
    }

    [Fact]
    public void Reset_SetsTheNextValueDirectly()
    {
        var store = new AutoIncrementStore();
        store.NextValue("orders");

        store.Reset("orders", 100);

        store.Peek("orders").Should().Be(100);
        store.NextValue("orders").Should().Be(101);
    }

    [Fact]
    public void Counters_AreIndependent_PerCollectionName()
    {
        var store = new AutoIncrementStore();

        store.NextValue("orders").Should().Be(1);
        store.NextValue("customers").Should().Be(1);
        store.NextValue("orders").Should().Be(2);
    }
}
