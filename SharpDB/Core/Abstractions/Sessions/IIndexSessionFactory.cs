namespace SharpDB.Core.Abstractions.Sessions;

public interface IIndexSessionFactory<TK> where TK : IComparable<TK>
{
    IIndexIOSession<TK> CreateSession(int indexId);
}