namespace SharpDB.V2.Engine.Primitives;

public interface IPoolable
{
    void Reset();
}

public sealed class ObjectPool<T> where T : class, IPoolable
{
}
