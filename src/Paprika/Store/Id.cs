using System.Numerics;

namespace Paprika.Store;

/// <summary>
/// The ID encoding utility.
/// </summary>
public static class Id
{
    public const int Limit = 1 << Shift;

    private const int IdSize = 4;
    private const int Shift = 6;

    /// <summary>
    /// Writes the id in a dense way, reducing the number of bytes needed to encode it.
    /// The padding does not break it.
    /// It writes it in a way that can be appended at the end with any payload and it will be unique.
    /// </summary>
    public static ReadOnlySpan<byte> WriteId(uint id)
    {
        Span<byte> span = stackalloc byte[IdSize];

        // The highest 2 bits are used to encode how many bytes are there. The rest, 6 bits are free to be used.
        var (extracted, prefix) = Math.DivRem(id, Limit);
        var bytesCount = (BitOperations.LeadingZeroCount(extracted) + 7) / 8;

        span[0] = (byte)((bytesCount << Shift) | prefix);

        var i = 1;
        while (extracted != 0)
        {
            span[i++] = (byte)(extracted & 0xFF);
            extracted >>= 8;
        }

        // TODO: remove allocation
        return span[..i].ToArray();
    }
}