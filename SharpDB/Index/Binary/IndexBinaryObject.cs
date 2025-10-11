namespace SharpDB.Index.Binary;

public interface INdexBinaryObject<TO>
{
    TO AsObject();
    int Size();
    byte[] GetBytes();
    TO GetFirst();
    TO GetNext(TO current);
    TO GetNext();
}