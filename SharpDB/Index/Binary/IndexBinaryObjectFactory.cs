namespace SharpDB.Index.Binary;

public interface INdexBinaryObjectFactory<TO>
{
    INdexBinaryObject<TO> Create(TO to);
    INdexBinaryObject<TO> Create(byte[] bytes, int beginning);
    INdexBinaryObject<TO> Create(byte[] bytes) => Create(bytes, 0);
    INdexBinaryObject<TO> CreateEmpty();
    int Size();
    Type GetType();
}