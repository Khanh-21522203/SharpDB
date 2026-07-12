namespace SharpDB.V2.Engine.Schema;

public enum SchemaCompatibilityKind
{
    /// <summary>The new schema can read existing rows without rewriting or filling additional data.</summary>
    Compatible,

    /// <summary>Existing rows remain readable, but new required data must be populated before the schema is fully usable.</summary>
    RequiresBackfill,

    /// <summary>Existing row bytes or index/partition layout must be rewritten before the new schema can be used safely.</summary>
    RequiresRewrite,

    /// <summary>The transition is not a valid evolution of the same collection schema.</summary>
    Forbidden
}

public sealed class SchemaCompatibility
{
    public required SchemaCompatibilityKind Kind { get; init; }
    public required string Reason { get; init; }

    public static SchemaCompatibility Compatible(string reason = "Schemas are compatible.") =>
        new() { Kind = SchemaCompatibilityKind.Compatible, Reason = reason };

    public static SchemaCompatibility RequiresBackfill(string reason) =>
        new() { Kind = SchemaCompatibilityKind.RequiresBackfill, Reason = reason };

    public static SchemaCompatibility RequiresRewrite(string reason) =>
        new() { Kind = SchemaCompatibilityKind.RequiresRewrite, Reason = reason };

    public static SchemaCompatibility Forbidden(string reason) =>
        new() { Kind = SchemaCompatibilityKind.Forbidden, Reason = reason };
}
