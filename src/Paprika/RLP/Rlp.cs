using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

using Nethermind.Int256;

namespace Paprika.RLP;

public static class Rlp
{
    public const int LengthOfKeccakRlp = 33;
    public const int MaxLengthOfLength = 4;
    public const int SmallPrefixBarrier = 56;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int LengthOf(in UInt256 item)
    {
        if (item < 128UL)
        {
            return 1;
        }

        return LongerLengthOf(item);
    }

    [SkipLocalsInit]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LongerLengthOf(in UInt256 item)
    {
        Vector256<byte> data;
        Unsafe.SkipInit(out data);

        Span<byte> bytes = MemoryMarshal.CreateSpan(ref Unsafe.As<Vector256<byte>, byte>(ref data), Vector256<byte>.Count);

        item.ToBigEndian(bytes);

        // at least one will be set as the first check is above, zero would not pass it
        var index = bytes.IndexOfAnyExcept((byte)0);
        var lengthWithoutLeadingZeroes = Vector256<byte>.Count - index;
        return lengthWithoutLeadingZeroes + 1;
    }

    public static int LengthOf(ReadOnlySpan<byte> span)
    {
        if (span.Length == 0 || span.Length == 1 && span[0] < 128)
        {
            return 1;
        }

        if (span.Length < SmallPrefixBarrier)
        {
            return span.Length + 1;
        }

        return LengthOfLength(span.Length) + 1 + span.Length;
    }

    public static int LengthOfSequence(int contentLength)
    {
        if (contentLength < SmallPrefixBarrier)
        {
            return 1 + contentLength;
        }

        return 1 + contentLength + LengthOfLength(contentLength);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public static int LengthOfLength(int value)
    {
        int bits = 32 - BitOperations.LeadingZeroCount((uint)value | 1);
        return (bits + 7) / 8;
    }
}
