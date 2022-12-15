using System.Runtime.InteropServices;

namespace Tree.Tests;

public struct Keccak
{
    public long Field0;
    public long Field1;
    public long Field2;
    public long Field3;

    public Span<byte> AsSpan() => MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref Field0, 4));
}