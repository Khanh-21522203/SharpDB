using System;
using SharpDB.Core.Abstractions.Serialization;

namespace SharpDB.Serialization;

/// <summary>
/// Serializer for System.Guid type
/// Guid is 16 bytes (128 bits)
/// </summary>
public class GuidSerializer : ISerializer<Guid>
{
    private const int GuidSize = 16; // Guid is always 16 bytes
    
    public int Size => GuidSize;

    public byte[] Serialize(Guid value)
    {
        // Convert Guid to byte array (16 bytes)
        return value.ToByteArray();
    }

    public Guid Deserialize(byte[] buffer, int offset = 0)
    {
        if (buffer == null)
            throw new ArgumentNullException(nameof(buffer));
            
        if (buffer.Length - offset < GuidSize)
            throw new ArgumentException($"Buffer must have at least {GuidSize} bytes from offset {offset}", nameof(buffer));
        
        // Create a new array for the Guid bytes
        var guidBytes = new byte[GuidSize];
        Array.Copy(buffer, offset, guidBytes, 0, GuidSize);
        
        // Create Guid from byte array
        return new Guid(guidBytes);
    }
}
