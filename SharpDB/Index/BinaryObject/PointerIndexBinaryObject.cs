using SharpDB.Core.Abstractions.Index;
using SharpDB.DataStructures;

namespace SharpDB.Index.BinaryObject;

/// <summary>
/// Binary object for Pointer (13 bytes).
/// Used in cluster indexes.
/// </summary>
public class PointerIndexBinaryObject : AbstractIndexBinaryObject<Pointer>
{
    public const int Bytes = Pointer.ByteSize; // 13
    
    public PointerIndexBinaryObject(byte[] bytes) : base(bytes)
    {
    }
    
    public PointerIndexBinaryObject(Pointer value) : base()
    {
        value.FillBytes(_bytes, 0);
    }
    
    public override Pointer AsObject()
    {
        return Pointer.FromBytes(_bytes, 0);
    }
    
    public override int Size => Bytes;
    
    public class Factory : IIndexBinaryObjectFactory<Pointer>
    {
        public IIndexBinaryObject<Pointer> Create(Pointer obj)
        {
            return new PointerIndexBinaryObject(obj);
        }
        
        public IIndexBinaryObject<Pointer> Create(byte[] bytes, int offset)
        {
            var objBytes = new byte[Bytes];
            Array.Copy(bytes, offset, objBytes, 0, Bytes);
            return new PointerIndexBinaryObject(objBytes);
        }
        
        public IIndexBinaryObject<Pointer> CreateEmpty()
        {
            return new PointerIndexBinaryObject(Pointer.Empty());
        }
        
        public int Size => Bytes;
        public Type Type => typeof(Pointer);
    }
}