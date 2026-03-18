namespace SharpDB.Engine;

public sealed class ForeignKeyConstraint
{
    public string FieldName { get; set; } = "";
    public string ReferencedCollection { get; set; } = "";
    public string ReferencedField { get; set; } = "";
    public bool AllowNull { get; set; }
}
