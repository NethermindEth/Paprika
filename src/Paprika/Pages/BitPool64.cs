using System.Numerics;

namespace Paprika.Pages;

/// <summary>
/// Represents a simple pool of up to 32 items.
/// </summary>
/// <remarks>
/// Finding a settable bit is done with a fast <see cref="BitOperations.LeadingZeroCount(uint)"/>.
/// </remarks>
public struct BitPool64
{
    public const int Size = sizeof(ulong);
    private const ulong One = 1UL;
    private const int BitsPerPool = 64;

    private ulong _value;

    /// <summary>
    /// Tries to set the first bit that is not set.
    /// </summary>
    /// <param name="maxBitToReserveExclusive">The number of bits used from the mask.</param>
    /// <param name="reserved">Which bit is set.</param>
    /// <returns>Whether setting succeeded.</returns>
    public bool TrySetLowestBit(int maxBitToReserveExclusive, out byte reserved)
    {
        var count = BitOperations.LeadingZeroCount(~_value);

        if (count < maxBitToReserveExclusive)
        {
            _value |= One << (BitsPerPool - count - 1);
            reserved = (byte)count;
            return true;
        }

        reserved = default;
        return false;
    }

    /// <summary>
    /// Clears the given bit, without asserting whether it was set first
    /// </summary>
    /// <param name="bit">The bit to be cleared.</param>
    public void ClearBit(byte bit) => _value &= ~(One << (BitsPerPool - bit - 1));
}