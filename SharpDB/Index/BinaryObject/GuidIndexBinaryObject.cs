using SharpDB.Core.Abstractions.Index;

namespace SharpDB.Index.BinaryObject;

public class GuidIndexBinaryObject : AbstractIndexBinaryObject<Guid>
{
    public const int Bytes = 16;
    
    public GuidIndexBinaryObject(byte[] bytes) : base(bytes)
    {
    }
    
    public GuidIndexBinaryObject(Guid value) : base()
    {
        value.ToByteArray().CopyTo(_bytes, 0);
    }
    
    public override Guid AsObject()
    {
        return new Guid(_bytes);
    }
    
    public override int Size => Bytes;
    
    public class Factory : IIndexBinaryObjectFactory<Guid>
    {
        public IIndexBinaryObject<Guid> Create(Guid obj)
        {
            return new GuidIndexBinaryObject(obj);
        }
        
        public IIndexBinaryObject<Guid> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[Bytes];
            Array.Copy(bytes, offset, objBytes, 0, Bytes);
            return new GuidIndexBinaryObject(objBytes);
        }
        
        public IIndexBinaryObject<Guid> CreateEmpty()
        {
            return new GuidIndexBinaryObject(Guid.Empty);
        }
        
        public int Size => Bytes;
        public Type Type => typeof(Guid);
    }
}