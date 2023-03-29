using System.Numerics;

namespace Paprika;

public static class BitExtensions
{
    private const int BitsPerUint = 32;
    
    public static bool TryReserveBit(ref uint bitmask, int maxBitToReserveExclusive, out byte reserved)
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
}