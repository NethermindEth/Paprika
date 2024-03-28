namespace Paprika.Utils;

public static class SpanExtensions
{
    private static readonly byte[] ZeroByte = [0];

    public static Span<byte> WithoutLeadingZeros(this Span<byte> bytes)
    {
        if (bytes.Length == 0)
        {
            return ZeroByte;
        }

        int nonZeroIndex = bytes.IndexOfAnyExcept((byte)0);

        return nonZeroIndex < 0 ? bytes[^1..] : bytes[nonZeroIndex..];
    }
}
