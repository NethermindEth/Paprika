namespace Paprika.Utils;

public static class SpanExtensions
{
    public static Span<byte> WithoutLeadingZeros(this Span<byte> bytes)
    {
        if (bytes.Length == 0)
        {
            return new byte[] { 0 };
        }

        int nonZeroIndex = bytes.IndexOfAnyExcept((byte)0);

        return nonZeroIndex < 0 ? bytes[^1..] : bytes[nonZeroIndex..];
    }
}
