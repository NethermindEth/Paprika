using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Crypto;

/// <summary>
/// Represents the value of a keccak.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public readonly struct Keccak : IEquatable<Keccak>
{
    public const int Size = 32;

    [FieldOffset(0)]
    private readonly ulong u1;
    [FieldOffset(8)]
    private readonly ulong u2;
    [FieldOffset(16)]
    private readonly ulong u3;
    [FieldOffset(24)]
    private readonly ulong u4;

    public Span<byte> BytesAsSpan => MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref Unsafe.AsRef(in u1), 4));

    public bool Equals(Keccak other) => u1 == other.u1 && u2 == other.u2 && u3 == other.u3 && u4 == other.u4;

    public override bool Equals(object? obj) => obj is Keccak other && Equals(other);

    public override int GetHashCode() => (int)(u1 ^ u2 ^ u3 ^ u4);

    public static bool operator ==(Keccak left, Keccak right) => left.Equals(right);

    public static bool operator !=(Keccak left, Keccak right) => !left.Equals(right);

    /// <returns>
    ///     <string>0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470</string>
    /// </returns>
    public static readonly Keccak OfAnEmptyString = Compute(Span<byte>.Empty);

    [DebuggerStepThrough]
    public static Keccak Compute(ReadOnlySpan<byte> input)
    {
        if (input.Length == 0)
        {
            return OfAnEmptyString;
        }

        Keccak result = new();
        KeccakHash.ComputeHashBytesToSpan(input, result.BytesAsSpan);
        return result;
    }
}