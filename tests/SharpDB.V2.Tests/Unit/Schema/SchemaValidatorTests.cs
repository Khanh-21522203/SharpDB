namespace SharpDB.V2.Tests.Unit.Schema;

using FluentAssertions;
using SharpDB.V2.Engine.Partitioning;
using SharpDB.V2.Engine.Schema;
using Xunit;

public sealed class SchemaValidatorTests
{
    [Fact]
    public void Validate_AllowsAWellFormedSchema()
    {
        var schema = OrdersSchema(1);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_WhenPrimaryKeyIsMissingFromColumns()
    {
        var schema = new CollectionSchema
        {
            Name = "orders",
            SchemaVersion = 1,
            PrimaryKeyFieldPath = "Missing",
            Columns = [IdColumn()],
        };

        var act = () => SchemaValidator.Validate(schema);

        act.Should().Throw<NotSupportedException>().WithMessage("*primary key*Missing*");
    }

    [Fact]
    public void Validate_Throws_WhenIndexReferencesUnknownColumn()
    {
        var schema = OrdersSchema(1, indexes:
        [
            new IndexDefinition { Name = "bad", Fields = [new IndexFieldDefinition { FieldPath = "Missing" }] },
        ]);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().Throw<NotSupportedException>().WithMessage("*bad*Missing*");
    }

    [Fact]
    public void Compare_ReturnsCompatible_WhenOnlyNullableColumnIsAppended()
    {
        var oldSchema = OrdersSchema(1);
        var newSchema = OrdersSchema(2, columns:
        [
            IdColumn(),
            CustomerColumn(),
            new ColumnDefinition { Name = "Email", FieldPath = "Email", TypeName = "String", IsNullable = true },
        ]);

        SchemaValidator.Compare(oldSchema, newSchema).Kind.Should().Be(SchemaCompatibilityKind.Compatible);
    }

    [Fact]
    public void Validate_AllowsCompositeSecondaryIndexes()
    {
        var schema = OrdersSchema(1, indexes:
        [
            new IndexDefinition
            {
                Name = "by_customer_id",
                Fields =
                [
                    new IndexFieldDefinition { FieldPath = "CustomerId" },
                    new IndexFieldDefinition { FieldPath = "Id", Ascending = false },
                ],
            },
        ]);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_WhenIndexHasDuplicateFields()
    {
        var schema = OrdersSchema(1, indexes:
        [
            new IndexDefinition
            {
                Name = "bad",
                Fields =
                [
                    new IndexFieldDefinition { FieldPath = "CustomerId" },
                    new IndexFieldDefinition { FieldPath = "CustomerId" },
                ],
            },
        ]);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().Throw<NotSupportedException>().WithMessage("*duplicate field path*CustomerId*");
    }

    [Fact]
    public void Validate_AllowsGlobalSecondaryIndexWithOwnPartitionKey()
    {
        var schema = OrdersSchema(1, indexes:
        [
            new IndexDefinition
            {
                Name = "global_by_customer_id",
                Scope = IndexScope.Global,
                PartitionKeyPaths = ["CustomerId", "Id"],
                Fields =
                [
                    new IndexFieldDefinition { FieldPath = "CustomerId" },
                    new IndexFieldDefinition { FieldPath = "Id" },
                ],
            },
        ]);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_AllowsCompositeBasePartitionKey()
    {
        var schema = new CollectionSchema
        {
            Name = "orders",
            SchemaVersion = 1,
            PrimaryKeyFieldPath = "Id",
            Columns = [IdColumn(), CustomerColumn()],
            PartitionKind = PartitionKind.Hash,
            BasePartitionKeyPaths = ["CustomerId", "Id"],
        };

        var act = () => SchemaValidator.Validate(schema);

        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_Throws_WhenGlobalIndexPartitionKeyIsNotTheFirstField()
    {
        var schema = OrdersSchema(1, indexes:
        [
            new IndexDefinition
            {
                Name = "bad",
                Scope = IndexScope.Global,
                PartitionKeyPaths = ["CustomerId"],
                Fields =
                [
                    new IndexFieldDefinition { FieldPath = "Id" },
                    new IndexFieldDefinition { FieldPath = "CustomerId" },
                ],
            },
        ]);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().Throw<NotSupportedException>().WithMessage("*fields must start*partition key*");
    }

    [Fact]
    public void Validate_Throws_WhenGlobalIndexPartitionReferencesALocalIndex()
    {
        var schema = new CollectionSchema
        {
            Name = "orders",
            SchemaVersion = 1,
            PrimaryKeyFieldPath = "Id",
            Columns = [IdColumn(), CustomerColumn()],
            SecondaryIndexDefinitions =
            [
                new IndexDefinition
                {
                    Name = "by_customer",
                    Fields = [new IndexFieldDefinition { FieldPath = "CustomerId" }],
                },
            ],
            GlobalIndexPartitions =
            [
                new IndexPartitionDescriptor { IndexName = "by_customer", Id = 0 },
            ],
        };

        var act = () => SchemaValidator.Validate(schema);

        act.Should().Throw<NotSupportedException>().WithMessage("*unknown global index*by_customer*");
    }

    [Fact]
    public void Validate_Throws_WhenLocalIndexDeclaresPartitionKey()
    {
        var schema = OrdersSchema(1, indexes:
        [
            new IndexDefinition
            {
                Name = "bad",
                Scope = IndexScope.Local,
                PartitionKeyPaths = ["CustomerId"],
                Fields = [new IndexFieldDefinition { FieldPath = "CustomerId" }],
            },
        ]);

        var act = () => SchemaValidator.Validate(schema);

        act.Should().Throw<NotSupportedException>().WithMessage("*Local index*partition key*");
    }

    [Fact]
    public void Compare_ReturnsRequiresBackfill_WhenNonNullableColumnIsAppended()
    {
        var oldSchema = OrdersSchema(1);
        var newSchema = OrdersSchema(2, columns:
        [
            IdColumn(),
            CustomerColumn(),
            new ColumnDefinition { Name = "CreatedAt", FieldPath = "CreatedAt", TypeName = "DateTime" },
        ]);

        SchemaValidator.Compare(oldSchema, newSchema).Kind.Should().Be(SchemaCompatibilityKind.RequiresBackfill);
    }

    [Fact]
    public void Compare_ReturnsRequiresRewrite_WhenExistingColumnOrderChanges()
    {
        var oldSchema = OrdersSchema(1);
        var newSchema = OrdersSchema(2, columns:
        [
            CustomerColumn(),
            IdColumn(),
        ]);

        SchemaValidator.Compare(oldSchema, newSchema).Kind.Should().Be(SchemaCompatibilityKind.RequiresRewrite);
    }

    [Fact]
    public void Compare_ReturnsRequiresRewrite_WhenPartitionKindChanges()
    {
        var oldSchema = OrdersSchema(1);
        var newSchema = new CollectionSchema
        {
            Name = "orders",
            SchemaVersion = 2,
            PrimaryKeyFieldPath = "Id",
            Columns = [IdColumn(), CustomerColumn()],
            PartitionKind = PartitionKind.Hash,
            BasePartitionKeyPaths = ["Id", "CustomerId"],
        };

        SchemaValidator.Compare(oldSchema, newSchema).Kind.Should().Be(SchemaCompatibilityKind.RequiresRewrite);
    }

    [Fact]
    public void Compare_ReturnsForbidden_WhenVersionDoesNotIncrease()
    {
        var oldSchema = OrdersSchema(1);
        var newSchema = OrdersSchema(1);

        SchemaValidator.Compare(oldSchema, newSchema).Kind.Should().Be(SchemaCompatibilityKind.Forbidden);
    }

    private static CollectionSchema OrdersSchema(
        int version,
        IReadOnlyList<ColumnDefinition>? columns = null,
        IReadOnlyList<IndexDefinition>? indexes = null) =>
        new()
        {
            Name = "orders",
            SchemaVersion = version,
            PrimaryKeyFieldPath = "Id",
            Columns = columns ?? [IdColumn(), CustomerColumn()],
            SecondaryIndexDefinitions = indexes ?? [],
        };

    private static ColumnDefinition IdColumn() =>
        new() { Name = "Id", FieldPath = "Id", TypeName = "Int64", IsPrimaryKey = true };

    private static ColumnDefinition CustomerColumn() =>
        new() { Name = "CustomerId", FieldPath = "CustomerId", TypeName = "Int64" };
}
