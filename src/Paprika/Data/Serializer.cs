using Nethermind.Int256;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Paprika.Data;

public static class Serializer
{
    public const int Uint256Size = 32;
    private const int Uint256PrefixSize = 1;
    public const int MaxUint256SizeWithPrefix = Uint256Size + Uint256PrefixSize;
    private const bool BigEndian = true;

    public const int StorageValueMaxByteCount = MaxUint256SizeWithPrefix;

    private const int NotFound = -1;

    /// <summary>
    /// Writes <paramref name="value"/> without encoding the length, returning the leftover.
    /// The caller should store length elsewhere.
    /// </summary>
    public static int WriteWithLeftover(in UInt256 value, Span<byte> destination)
    {
        var slice = destination.Slice(0, Uint256Size);
        value.ToBigEndian(slice);
        var firstNonZero = slice.IndexOfAnyExcept((byte)0);

        if (firstNonZero == NotFound)
        {
            // all zeros
            return 0;
        }

        // some non-zeroes
        var length = Uint256Size - firstNonZero;

        // move left
        destination.Slice(firstNonZero, length).CopyTo(destination.Slice(0, length));
        return length;
    }

    [SkipLocalsInit]
    public static void ReadFrom(ReadOnlySpan<byte> source, out UInt256 value)
    {
        if (source.Length != Uint256Size)
        {
            // Clear the vector as we might not fill all of it
            Vector256<byte> data = default;
            Span<byte> uint256 = MemoryMarshal.CreateSpan(ref Unsafe.As<Vector256<byte>, byte>(ref data), Vector256<byte>.Count);
            source.CopyTo(uint256.Slice(Uint256Size - source.Length));
            source = uint256;
        }

        value = new UInt256(source, BigEndian);
    }
}
