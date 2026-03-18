namespace SharpDB.Engine;

public class Schema
{
    public List<Field> Fields { get; set; } = new();
    public List<ForeignKeyConstraint> ForeignKeys { get; set; } = new();
    public int Version { get; set; } = 1;
    public Field? PrimaryKey => Fields.FirstOrDefault(f => f.IsPrimaryKey);

    public Field? GetField(string name)
    {
        return Fields.FirstOrDefault(f => f.Name == name);
    }

    public void Validate()
    {
        if (Fields.Count == 0)
            throw new InvalidOperationException("Schema must have at least one field");

        if (PrimaryKey == null)
            throw new InvalidOperationException("Schema must have a primary key");

        // Check for duplicate field names
        var duplicates = Fields.GroupBy(f => f.Name)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicates.Any())
            throw new InvalidOperationException($"Duplicate field names: {string.Join(", ", duplicates)}");

        foreach (var fk in ForeignKeys)
        {
            if (string.IsNullOrWhiteSpace(fk.FieldName))
                throw new InvalidOperationException("Foreign key field name cannot be empty");

            if (string.IsNullOrWhiteSpace(fk.ReferencedCollection))
                throw new InvalidOperationException($"Foreign key '{fk.FieldName}' must define referenced collection");

            if (string.IsNullOrWhiteSpace(fk.ReferencedField))
                throw new InvalidOperationException($"Foreign key '{fk.FieldName}' must define referenced field");

            if (GetField(fk.FieldName) == null)
                throw new InvalidOperationException($"Foreign key field '{fk.FieldName}' not found in schema");
        }
    }

    public bool Matches(Type type)
    {
        foreach (var field in Fields)
        {
            var property = type.GetProperty(field.Name);
            if (property == null)
                return false;

            var expectedType = GetType(field.Type);
            if (expectedType != null && property.PropertyType != expectedType)
                return false;
        }

        return true;
    }

    private Type? GetType(FieldType fieldType)
    {
        return fieldType switch
        {
            FieldType.Int => typeof(int),
            FieldType.Long => typeof(long),
            FieldType.Double => typeof(double),
            FieldType.Bool => typeof(bool),
            FieldType.String => typeof(string),
            FieldType.DateTime => typeof(DateTime),
            FieldType.Binary => typeof(byte[]),
            _ => null
        };
    }
}
