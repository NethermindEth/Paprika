using System.Numerics;

namespace Paprika;

public static class BitExtensions
{
    private const int BitsPerUint = 32;

    /// <summary>
    /// Tries to set the first bit that is not set.
    /// </summary>
    /// <param name="bitmask">The bitmask to operate on.</param>
    /// <param name="maxBitToReserveExclusive">The number of bits used from the mask.</param>
    /// <param name="reserved">Which bit is set.</param>
    /// <returns>Whether setting succeeded.</returns>
    public static bool TrySetLowestBit(ref uint bitmask, int maxBitToReserveExclusive, out byte reserved)
    {
        var count = BitOperations.LeadingZeroCount(~bitmask);

        if (count < maxBitToReserveExclusive)
        {
            bitmask |= 1U << (BitsPerUint - count - 1);
            reserved = (byte)count;
            return true;
        }

        reserved = default;
        return false;
    }

    /// <summary>
    /// Clears the set bit.
    /// </summary>
    /// <param name="bitmask">The bitmask to operate on.</param>
    /// <param name="bit">The bit to be cleared.</param>
    public static void ClearBit(ref uint bitmask, byte bit) => bitmask &= ~(1U << (BitsPerUint - bit - 1));
}