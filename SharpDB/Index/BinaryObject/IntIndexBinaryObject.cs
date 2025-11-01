using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

public class IntIndexBinaryObject : AbstractIndexBinaryObject<int>
{
    public const int Bytes = sizeof(int); // 4
    
    public IntIndexBinaryObject(byte[] bytes) : base(bytes)
    {
    }
    
    public IntIndexBinaryObject(int value) : base()
    {
        BitConverter.GetBytes(value).CopyTo(_bytes, 0);
    }
    
    public override int AsObject()
    {
        return BitConverter.ToInt32(_bytes, 0);
    }
    
    public override int Size => Bytes;
    
    public class Factory : IIndexBinaryObjectFactory<int>
    {
        public IIndexBinaryObject<int> Create(int obj)
        {
            return new IntIndexBinaryObject(obj);
        }
        
        public IIndexBinaryObject<int> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[Bytes];
            Array.Copy(bytes, offset, objBytes, 0, Bytes);
            return new IntIndexBinaryObject(objBytes);
        }
        
        public IIndexBinaryObject<int> CreateEmpty()
        {
            return new IntIndexBinaryObject(0);
        }
        
        public int Size => Bytes;
        public Type Type => typeof(int);
    }
}