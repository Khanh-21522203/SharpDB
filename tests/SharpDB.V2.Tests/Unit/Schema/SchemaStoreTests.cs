namespace SharpDB.V2.Tests.Unit.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using SharpDB.V2.Engine.Schema;
using SharpDB.V2.Engine.Types;
using Xunit;

public sealed class SchemaStoreTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "sharpdb-schema-tests-" + Guid.NewGuid());

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Fact]
    public void TryLoad_ReturnsNull_ForUnknownCollection()
    {
        var store = new SchemaStore(_directory);

        store.TryLoad("orders").Should().BeNull();
    }

    [Fact]
    public void Exists_IsFalse_BeforeSave_AndTrue_AfterSave()
    {
        var store = new SchemaStore(_directory);
        var schema = new CollectionSchema { Name = "orders" };

        store.Exists("orders").Should().BeFalse();
        store.Save(schema);
        store.Exists("orders").Should().BeTrue();
    }

    [Fact]
    public void NewSchema_DefaultsToUnpartitioned()
    {
        var schema = new CollectionSchema { Name = "orders" };

        schema.PartitionKind.Should().Be(PartitionKind.None);
        schema.SchemaVersion.Should().Be(1);
        schema.PrimaryKeyFieldPath.Should().Be("");
        schema.Columns.Should().BeEmpty();
        schema.BasePartitionKeyPaths.Should().BeEmpty();
        schema.Partitions.Should().BeEmpty();
    }

    [Fact]
    public void Delete_RemovesTheSchema_AndIsANoOp_WhenMissing()
    {
        var store = new SchemaStore(_directory);
        store.Save(new CollectionSchema { Name = "orders" });

        store.Delete("orders");

        store.Exists("orders").Should().BeFalse();
        var act = () => store.Delete("orders");
        act.Should().NotThrow();
    }

    [Fact]
    public void SaveThenTryLoad_RoundTrips_MultiplePartitionsAndSecondaryIndexes()
    {
        var store = new SchemaStore(_directory);
        var schema = new CollectionSchema
        {
            Name = "orders",
            SchemaVersion = 3,
            PrimaryKeyFieldPath = "Id",
            Columns =
            [
                new ColumnDefinition { Name = "Id", FieldPath = "Id", TypeName = "Int64", IsPrimaryKey = true },
                new ColumnDefinition { Name = "CustomerId", FieldPath = "CustomerId", TypeName = "Int64" },
                new ColumnDefinition { Name = "Email", FieldPath = "Email", TypeName = "String", IsNullable = true },
            ],
            PartitionKind = PartitionKind.Range,
            BasePartitionKeyPaths = ["CreatedAt", "Id"],
            SecondaryIndexDefinitions =
            [
                new IndexDefinition
                {
                    Name = "by_customer",
                    IsUnique = false,
                    Fields = [new IndexFieldDefinition { FieldPath = "CustomerId" }],
                },
                new IndexDefinition
                {
                    Name = "by_email",
                    IsUnique = true,
                    Fields = [new IndexFieldDefinition { FieldPath = "Email", Ascending = false }],
                },
                new IndexDefinition
                {
                    Name = "by_customer_email",
                    Fields =
                    [
                        new IndexFieldDefinition { FieldPath = "CustomerId" },
                        new IndexFieldDefinition { FieldPath = "Email", Ascending = false },
                    ],
                },
                new IndexDefinition
                {
                    Name = "global_by_customer_created",
                    Scope = IndexScope.Global,
                    PartitionKeyPaths = ["CustomerId", "Email"],
                    Fields =
                    [
                        new IndexFieldDefinition { FieldPath = "CustomerId" },
                        new IndexFieldDefinition { FieldPath = "Email" },
                    ],
                },
            ],
            GlobalIndexPartitions =
            [
                new IndexPartitionDescriptor
                {
                    IndexName = "global_by_customer_created",
                    Id = 0,
                    Name = "g0",
                    RootPageId = new PageId(100),
                },
            ],
            Partitions =
            [
                new DataPartitionDescriptor
                {
                    Id = 0,
                    Name = "p0",
                    LowerBoundKey = null,
                    UpperBoundKey = [1, 0, 0, 0],
                    PrimaryIndexRootPageId = new PageId(10),
                    SecondaryIndexRootPageIds = new Dictionary<string, PageId>
                    {
                        ["by_customer"] = new PageId(11),
                        ["by_email"] = new PageId(12),
                    },
                },
                new DataPartitionDescriptor
                {
                    Id = 1,
                    Name = "p1",
                    LowerBoundKey = [1, 0, 0, 0],
                    UpperBoundKey = null,
                    PrimaryIndexRootPageId = new PageId(20),
                    SecondaryIndexRootPageIds = new Dictionary<string, PageId>
                    {
                        ["by_customer"] = new PageId(21),
                        ["by_email"] = new PageId(22),
                    },
                },
            ],
        };

        store.Save(schema);
        var reloaded = store.TryLoad("orders");

        reloaded.Should().BeEquivalentTo(schema);
    }
}
