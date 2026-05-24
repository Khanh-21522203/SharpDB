using System.Runtime.InteropServices;

namespace SharpDB.V2.Engine.Primitives;

[StructLayout(LayoutKind.Explicit, Size = 128)]
public struct CacheLinePadded<T> where T : struct
{
    [FieldOffset(64)]
    public T Value;
}
