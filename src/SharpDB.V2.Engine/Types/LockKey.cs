namespace SharpDB.V2.Engine.Types;

public readonly record struct LockKey(string Collection, long RecordId);
