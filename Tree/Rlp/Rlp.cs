namespace Tree.Rlp;

public static class Rlp
{
    public static int LengthOf(ReadOnlySpan<byte> span)
    {
        if (span.Length == 0)
        {
            return 1;
        }

        if (span.Length == 1 && span[0] < 128)
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

        return 1 + LengthOfLength(contentLength) + contentLength;
    }

    public static int LengthOfLength(int value)
    {
        // BitOperations.LeadingZeroCount((value)
        
        if (value < 1 << 8)
        {
            return 1;
        }

        if (value < 1 << 16)
        {
            return 2;
        }

        if (value < 1 << 24)
        {
            return 3;
        }

        return 4;
    }
}