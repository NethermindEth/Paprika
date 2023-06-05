using Nethermind.Int256;
using Paprika.Utils;

namespace Paprika.RLP;

public static class Rlp
{
    public const int LengthOfKeccakRlp = 33;

    public static int LengthOf(UInt256 item)
    {
        if (item < 128UL)
        {
            return 1;
        }

        Span<byte> bytes = stackalloc byte[32];
        item.ToBigEndian(bytes);
        int length = bytes.WithoutLeadingZeros().Length;
        return length + 1;
    }

    public static int LengthOf(ReadOnlySpan<byte> span)
    {
        if (span.Length == 0 || (span.Length == 1 && span[0] < 128))
        {
            return 1;
        }

        if (span.Length < 56)
        {
            return span.Length + 1;
        }

        return LengthOfLength(span.Length) + 1 + span.Length;
    }

    public static int LengthOfSequence(int contentLength)
    {
        if (contentLength < 56)
        {
            return 1 + contentLength;
        }
        else
        {
            return 1 + contentLength + LengthOfLength(contentLength);
        }
    }

    public static int LengthOfLength(int value)
    {
        if (value < 1 << 8)
        {
            return 1;
        }
        else if (value < 1 << 16)
        {
            return 2;
        }
        else if (value < 1 << 24)
        {
            return 3;
        }
        else
        {
            return 4;
        }
    }
}
