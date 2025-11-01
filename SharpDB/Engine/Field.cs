namespace SharpDB.Engine;

public class Field
{
    public string Name { get; set; } = "";
    public FieldType Type { get; set; }
    public int? MaxLength { get; set; } 
    public bool IsPrimaryKey { get; set; }
    public bool IsIndexed { get; set; }
    public bool IsUnique { get; set; }
    public bool IsNullable { get; set; }
    public bool IsAutoIncrement { get; set; }

    public int GetSize()
    {
        return Type switch
        {
            FieldType.Int => sizeof(int),
            FieldType.Long => sizeof(long),
            FieldType.Double => sizeof(double),
            FieldType.Bool => sizeof(bool),
            FieldType.String => MaxLength ?? 255,
            FieldType.DateTime => sizeof(long),  // Ticks
            _ => throw new NotSupportedException($"Field type {Type} not supported")
        };
    }
}

public enum FieldType
{
    Int,
    Long,
    Double,
    Bool,
    String,
    DateTime,
    Binary
}