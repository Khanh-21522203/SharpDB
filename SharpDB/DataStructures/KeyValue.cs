namespace SharpDB.DataStructures;

/// <summary>
/// Key-value pair used in B+ Tree leaf nodes.
/// </summary>
public record KeyValue<TK, TV>(TK Key, TV Value) : IComparable<KeyValue<TK, TV>>
    where TK : IComparable<TK>
{
    public int CompareTo(KeyValue<TK, TV>? other)
    {
        if (other == null) return 1;
        return Key.CompareTo(other.Key);
    }
    
    public void Deconstruct(out TK key, out TV value)
    {
        key = Key;
        value = Value;
    }
    
    public override string ToString() => $"({Key}, {Value})";
}