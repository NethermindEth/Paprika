using System.Numerics;

namespace Paprika;

public static class BitExtensions
{
    public static bool TryReserveBit(ref uint bitmask, int maxBitToReserveExclusive, out byte reserved)
    {
        var count = BitOperations.LeadingZeroCount(~bitmask);
        
        if (count < maxBitToReserveExclusive)
        {
            bitmask |= 1U << count;
            reserved = (byte)count;
            return true;
        }

        reserved = default;
        return false;
    }
}