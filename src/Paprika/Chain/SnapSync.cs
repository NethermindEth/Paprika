using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Chain;

public static class SnapSync
{
    public const int BoundaryValueSize = Keccak.Size + PreambleLength;
    private const byte Byte0 = 0;
    private const byte Byte1 = 0;
    public const int PreambleLength = 2;

    public static Span<byte> WriteBoundaryValue(in Keccak value, in Span<byte> payload)
    {
        payload[0] = Byte0;
        payload[1] = Byte1;
        value.Span.CopyTo(payload[PreambleLength..]);

        return payload.Slice(0, BoundaryValueSize);
    }

    public static bool IsBoundaryHashLeaf(in Node.Leaf leaf)
    {
        return leaf.Path.Length > 64;
    }

    public static bool IsBoundaryValue(in ReadOnlySpan<byte> value)
    {
        return value.Length == BoundaryValueSize && value[0] == Byte0 && value[1] == Byte1;
    }

    public static bool TryGetKeccakFromBoundaryPath(in NibblePath path, out Keccak keccak)
    {
        if (path.Length > 64)
        {
            keccak = default;
            var rem = path.SliceFrom(path.Length - 64);
            rem.WriteToKeccak(keccak.BytesAsSpan);
            return true;
        }
        keccak = default;
        return false;
    }
}
