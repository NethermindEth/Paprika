using System.Runtime.InteropServices;

namespace Paprika;

/// <summary>
/// Represents the value of a keccak.
/// </summary>
public unsafe struct Keccak
{
    public const int Size = 32;
    public fixed byte Bytes[Size];

    public Span<byte> BytesAsSpan => MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref this, 1));
}