using System.Buffers.Binary;

namespace Paprika.Data;

/// <summary>
/// Provides shared capabilities to write and read spans.
/// </summary>
public static class SpanSerialization
{
    private const int PrefixLength = 2;

    /// <summary>
    /// Gets the maximum number of bytes needed to write this span
    /// </summary>
    /// <param name="span"></param>
    /// <returns></returns>
    public static int MaxByteLength(this ReadOnlySpan<byte> span) => PrefixLength + span.Length;

    /// <summary>
    /// Reads the span from destination, returning the leftover.
    /// </summary>
    /// <returns>The leftover</returns>
    public static ReadOnlySpan<byte> ReadFrom(this ReadOnlySpan<byte> source, out ReadOnlySpan<byte> value)
    {
        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        value = source.Slice(PrefixLength, length);
        return source.Slice(PrefixLength + length);
    }

    public static int ReadLength(this ReadOnlySpan<byte> source)
    {
        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        return PrefixLength + length;
    }

    /// <summary>
    /// Writes the span to the destination.
    /// </summary>
    /// <returns>The leftover.</returns>
    public static Span<byte> WriteToWithLeftover(this ReadOnlySpan<byte> span, Span<byte> destination)
    {
        var length = (ushort)span.Length;
        BinaryPrimitives.WriteUInt16LittleEndian(destination, length);
        span.CopyTo(destination.Slice(2));
        return destination.Slice(PrefixLength + length);
    }
}

