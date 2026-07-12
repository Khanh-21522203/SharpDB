namespace SharpDB.V2.Engine.Schema;

public static class SchemaValidator
{
    private static readonly HashSet<string> SupportedTypeNames = new(StringComparer.Ordinal)
    {
        "Int32",
        "Int64",
        "Double",
        "Boolean",
        "Decimal",
        "Guid",
        "DateTime",
        "DateTimeOffset",
        "String",
        "ByteArray"
    };

    public static void Validate(CollectionSchema schema)
    {
        if (string.IsNullOrWhiteSpace(schema.Name))
        {
            throw new NotSupportedException("Collection schema name is required.");
        }

        if (schema.SchemaVersion <= 0)
        {
            throw new NotSupportedException("Collection schema version must be positive.");
        }

        ValidateColumns(schema);
        ValidateIndexes(schema);
        ValidatePartitioning(schema);
    }

    public static SchemaCompatibility Compare(CollectionSchema oldSchema, CollectionSchema newSchema)
    {
        Validate(oldSchema);
        Validate(newSchema);

        if (!string.Equals(oldSchema.Name, newSchema.Name, StringComparison.Ordinal))
        {
            return SchemaCompatibility.Forbidden("Collection name cannot change.");
        }

        if (newSchema.SchemaVersion <= oldSchema.SchemaVersion)
        {
            return SchemaCompatibility.Forbidden("New schema version must be greater than the old schema version.");
        }

        if (!string.Equals(oldSchema.PrimaryKeyFieldPath, newSchema.PrimaryKeyFieldPath, StringComparison.Ordinal))
        {
            return SchemaCompatibility.RequiresRewrite("Changing the primary key requires rewriting rows and indexes.");
        }

        if (oldSchema.PartitionKind != newSchema.PartitionKind ||
            !SameFieldPathList(ResolveBasePartitionKeyPaths(oldSchema), ResolveBasePartitionKeyPaths(newSchema)))
        {
            return SchemaCompatibility.RequiresRewrite("Changing partition strategy or partition key requires repartitioning data.");
        }

        var oldColumns = oldSchema.Columns;
        var newColumns = newSchema.Columns;
        if (newColumns.Count < oldColumns.Count)
        {
            return SchemaCompatibility.RequiresRewrite("Removing columns requires rewriting rows.");
        }

        for (var i = 0; i < oldColumns.Count; i++)
        {
            var oldColumn = oldColumns[i];
            var newColumn = newColumns[i];
            if (!SameColumnIdentity(oldColumn, newColumn))
            {
                return SchemaCompatibility.RequiresRewrite("Reordering or renaming existing columns requires rewriting rows.");
            }

            if (!string.Equals(oldColumn.TypeName, newColumn.TypeName, StringComparison.Ordinal))
            {
                return SchemaCompatibility.RequiresRewrite($"Changing column '{oldColumn.Name}' type requires rewriting rows.");
            }

            if (oldColumn.IsNullable && !newColumn.IsNullable)
            {
                return SchemaCompatibility.RequiresBackfill($"Column '{oldColumn.Name}' became non-nullable and requires validation/backfill.");
            }
        }

        for (var i = oldColumns.Count; i < newColumns.Count; i++)
        {
            if (!newColumns[i].IsNullable)
            {
                return SchemaCompatibility.RequiresBackfill($"New non-nullable column '{newColumns[i].Name}' requires backfill.");
            }
        }

        return SchemaCompatibility.Compatible();
    }

    private static void ValidateColumns(CollectionSchema schema)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var fieldPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (var column in schema.Columns)
        {
            if (string.IsNullOrWhiteSpace(column.Name))
            {
                throw new NotSupportedException("Column name is required.");
            }

            if (!names.Add(column.Name))
            {
                throw new NotSupportedException($"Collection schema '{schema.Name}' declares duplicate column '{column.Name}'.");
            }

            var fieldPath = string.IsNullOrWhiteSpace(column.FieldPath) ? column.Name : column.FieldPath;
            if (!fieldPaths.Add(fieldPath))
            {
                throw new NotSupportedException($"Collection schema '{schema.Name}' maps multiple columns to field path '{fieldPath}'.");
            }

            if (!string.IsNullOrWhiteSpace(column.TypeName) && !SupportedTypeNames.Contains(column.TypeName))
            {
                throw new NotSupportedException($"Column '{column.Name}' declares unsupported type '{column.TypeName}'.");
            }
        }

        if (!string.IsNullOrWhiteSpace(schema.PrimaryKeyFieldPath) &&
            !fieldPaths.Contains(schema.PrimaryKeyFieldPath))
        {
            throw new NotSupportedException(
                $"Collection schema '{schema.Name}' primary key '{schema.PrimaryKeyFieldPath}' is not present in the column list.");
        }

        var primaryKeyCount = schema.Columns.Count(c => c.IsPrimaryKey);
        if (primaryKeyCount > 1)
        {
            throw new NotSupportedException($"Collection schema '{schema.Name}' declares more than one primary key column.");
        }

        if (!string.IsNullOrWhiteSpace(schema.PrimaryKeyFieldPath) &&
            primaryKeyCount == 1 &&
            !schema.Columns.Any(c => c.IsPrimaryKey && FieldPathOf(c) == schema.PrimaryKeyFieldPath))
        {
            throw new NotSupportedException(
                $"Collection schema '{schema.Name}' primary key marker does not match PrimaryKeyFieldPath.");
        }
    }

    private static void ValidateIndexes(CollectionSchema schema)
    {
        var fieldPaths = schema.Columns.Select(FieldPathOf).ToHashSet(StringComparer.Ordinal);
        var indexNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var index in schema.SecondaryIndexDefinitions)
        {
            if (string.IsNullOrWhiteSpace(index.Name))
            {
                throw new NotSupportedException("Index name is required.");
            }

            if (!indexNames.Add(index.Name))
            {
                throw new NotSupportedException($"Collection schema '{schema.Name}' declares duplicate index '{index.Name}'.");
            }

            if (index.Fields.Count == 0)
            {
                throw new NotSupportedException($"Index '{index.Name}' must declare at least one field.");
            }

            if (index.Scope == IndexScope.Local && index.PartitionKeyPaths.Count > 0)
            {
                throw new NotSupportedException($"Local index '{index.Name}' cannot declare an index partition key.");
            }

            if (index.Scope == IndexScope.Global)
            {
                if (index.PartitionKeyPaths.Count == 0)
                {
                    throw new NotSupportedException($"Global index '{index.Name}' must declare an index partition key.");
                }

                foreach (var partitionKeyPath in index.PartitionKeyPaths)
                {
                    if (string.IsNullOrWhiteSpace(partitionKeyPath))
                    {
                        throw new NotSupportedException($"Global index '{index.Name}' has an empty partition key path.");
                    }

                    if (schema.Columns.Count > 0 && !fieldPaths.Contains(partitionKeyPath))
                    {
                        throw new NotSupportedException(
                            $"Global index '{index.Name}' partition key '{partitionKeyPath}' is not present in the column list.");
                    }
                }
            }

            var indexFieldPaths = new HashSet<string>(StringComparer.Ordinal);
            foreach (var field in index.Fields)
            {
                if (string.IsNullOrWhiteSpace(field.FieldPath))
                {
                    throw new NotSupportedException($"Index '{index.Name}' has an empty field path.");
                }

                if (!indexFieldPaths.Add(field.FieldPath))
                {
                    throw new NotSupportedException($"Index '{index.Name}' declares duplicate field path '{field.FieldPath}'.");
                }

                if (schema.Columns.Count > 0 && !fieldPaths.Contains(field.FieldPath))
                {
                    throw new NotSupportedException($"Index '{index.Name}' references unknown field path '{field.FieldPath}'.");
                }
            }

            if (index.Scope == IndexScope.Global && !StartsWithPartitionKey(index))
            {
                throw new NotSupportedException(
                    $"Global index '{index.Name}' fields must start with its partition key.");
            }
        }
    }

    private static void ValidatePartitioning(CollectionSchema schema)
    {
        var fieldPaths = schema.Columns.Select(FieldPathOf).ToHashSet(StringComparer.Ordinal);
        foreach (var basePartitionKeyPath in ResolveBasePartitionKeyPaths(schema))
        {
            if (string.IsNullOrWhiteSpace(basePartitionKeyPath))
            {
                throw new NotSupportedException("Base partition key path cannot be empty.");
            }

            if (schema.Columns.Count > 0 && !fieldPaths.Contains(basePartitionKeyPath))
            {
                throw new NotSupportedException(
                    $"Base partition key '{basePartitionKeyPath}' is not present in the column list.");
            }
        }

        if (schema.PartitionKind == Partitioning.PartitionKind.None && schema.Partitions.Count > 1)
        {
            throw new NotSupportedException("Unpartitioned collections cannot declare more than one partition.");
        }

        var partitionIds = new HashSet<int>();
        var partitionNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var partition in schema.Partitions)
        {
            if (!partitionIds.Add(partition.Id))
            {
                throw new NotSupportedException($"Collection schema '{schema.Name}' declares duplicate partition id '{partition.Id}'.");
            }

            if (!string.IsNullOrWhiteSpace(partition.Name) && !partitionNames.Add(partition.Name))
            {
                throw new NotSupportedException($"Collection schema '{schema.Name}' declares duplicate partition '{partition.Name}'.");
            }
        }

        ValidateGlobalIndexPartitions(schema);
    }

    private static void ValidateGlobalIndexPartitions(CollectionSchema schema)
    {
        var globalIndexNames = schema.SecondaryIndexDefinitions
            .Where(i => i.Scope == IndexScope.Global)
            .Select(i => i.Name)
            .ToHashSet(StringComparer.Ordinal);

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var partition in schema.GlobalIndexPartitions)
        {
            if (string.IsNullOrWhiteSpace(partition.IndexName))
            {
                throw new NotSupportedException("Global index partition must reference an index name.");
            }

            if (!globalIndexNames.Contains(partition.IndexName))
            {
                throw new NotSupportedException(
                    $"Global index partition references unknown global index '{partition.IndexName}'.");
            }

            var key = $"{partition.IndexName}:{partition.Id}";
            if (!seen.Add(key))
            {
                throw new NotSupportedException(
                    $"Global index '{partition.IndexName}' declares duplicate partition id '{partition.Id}'.");
            }
        }
    }

    private static bool SameColumnIdentity(ColumnDefinition left, ColumnDefinition right) =>
        string.Equals(left.Name, right.Name, StringComparison.Ordinal) &&
        string.Equals(FieldPathOf(left), FieldPathOf(right), StringComparison.Ordinal);

    private static string FieldPathOf(ColumnDefinition column) =>
        string.IsNullOrWhiteSpace(column.FieldPath) ? column.Name : column.FieldPath;

    private static IReadOnlyList<string> ResolveBasePartitionKeyPaths(CollectionSchema schema) =>
        schema.BasePartitionKeyPaths.Count == 0 && !string.IsNullOrWhiteSpace(schema.PrimaryKeyFieldPath)
            ? [schema.PrimaryKeyFieldPath]
            : schema.BasePartitionKeyPaths;

    private static bool StartsWithPartitionKey(IndexDefinition index)
    {
        if (index.Fields.Count < index.PartitionKeyPaths.Count)
        {
            return false;
        }

        for (var i = 0; i < index.PartitionKeyPaths.Count; i++)
        {
            if (!string.Equals(index.Fields[i].FieldPath, index.PartitionKeyPaths[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static bool SameFieldPathList(IReadOnlyList<string> left, IReadOnlyList<string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Count; i++)
        {
            if (!string.Equals(left[i], right[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}
