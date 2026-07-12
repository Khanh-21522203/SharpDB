namespace SharpDB.V2.Tests.Unit.Serialization;

using System;
using System.Collections.Generic;
using FluentAssertions;
using SharpDB.V2.Engine.Serialization;
using Xunit;

public sealed class KeyExtractorTests
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private struct LegacyComparableKey : IComparable
    {
        public int Value;
        public int CompareTo(object? obj) => Value.CompareTo(((LegacyComparableKey)obj!).Value);
    }

    private sealed class NonComparableKey;

    [Fact]
    public void ExtractKey_ReturnsSelectorResult()
    {
        var extractor = new KeyExtractor<Row, int>(r => r.Id);
        var row = new Row { Id = 42, Name = "x" };

        extractor.ExtractKey(row).Should().Be(42);
    }

    [Fact]
    public void CompareKeys_UsesDefaultComparer_ForGenericComparable()
    {
        var extractor = new KeyExtractor<Row, int>(r => r.Id);

        extractor.CompareKeys(1, 2).Should().BeLessThan(0);
        extractor.CompareKeys(2, 2).Should().Be(0);
        extractor.CompareKeys(3, 2).Should().BeGreaterThan(0);
    }

    [Fact]
    public void CompareKeys_UsesDefaultComparer_ForNonGenericComparable()
    {
        var extractor = new KeyExtractor<Row, LegacyComparableKey>(_ => default);

        var a = new LegacyComparableKey { Value = 1 };
        var b = new LegacyComparableKey { Value = 2 };

        extractor.CompareKeys(a, b).Should().BeLessThan(0);
        extractor.CompareKeys(b, a).Should().BeGreaterThan(0);
        extractor.CompareKeys(a, a).Should().Be(0);
    }

    [Fact]
    public void CompareKeys_UsesSuppliedComparer_WhenProvided()
    {
        var reverse = Comparer<int>.Create((x, y) => y.CompareTo(x));
        var extractor = new KeyExtractor<Row, int>(r => r.Id, reverse);

        extractor.CompareKeys(1, 2).Should().BeGreaterThan(0);
        extractor.CompareKeys(2, 1).Should().BeLessThan(0);
    }

    [Fact]
    public void Constructor_Throws_WhenKeySelectorIsNull()
    {
        var act = () => new KeyExtractor<Row, int>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_Throws_ForNonComparableKeyType_WithoutExplicitComparer()
    {
        var act = () => new KeyExtractor<Row, NonComparableKey>(_ => new NonComparableKey());
        act.Should().Throw<NotSupportedException>().WithMessage("*NonComparableKey*");
    }
}
