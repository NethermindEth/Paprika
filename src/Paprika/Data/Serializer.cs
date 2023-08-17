using Nethermind.Int256;

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
    public static Span<byte> WriteWithLeftover(this UInt256 value, Span<byte> destination, out int length)
    {
        var slice = destination.Slice(0, Uint256Size);
        value.ToBigEndian(slice);
        var firstNonZero = slice.IndexOfAnyExcept((byte)0);

        if (firstNonZero == NotFound)
        {
            // all zeros
            length = 0;
            return destination;
        }

        // some non-zeroes
        length = Uint256Size - firstNonZero;

        // move left
        destination.Slice(firstNonZero, length).CopyTo(destination.Slice(0, length));
        return destination.Slice(length);
    }

    public static void ReadFrom(ReadOnlySpan<byte> source, out UInt256 value)
    {
        Span<byte> uint256 = stackalloc byte[Uint256Size];

        source.CopyTo(uint256.Slice(Uint256Size - source.Length));
        value = new UInt256(uint256, BigEndian);
    }
}