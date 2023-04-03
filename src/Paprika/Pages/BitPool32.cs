using System.Numerics;

namespace Paprika.Pages;

/// <summary>
/// Represents a simple pool of up to 32 items.
/// </summary>
/// <remarks>
/// Finding a settable bit is done with a fast <see cref="BitOperations.LeadingZeroCount(uint)"/>.
/// </remarks>
public struct BitPool32
{
    public const int Size = sizeof(uint);

    private uint _value;

    private const int BitsPerUint = 32;

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
            _value |= 1U << (BitsPerUint - count - 1);
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
    public void ClearBit(byte bit) => _value &= ~(1U << (BitsPerUint - bit - 1));
}