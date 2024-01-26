using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Chain;

public static class SnapSync
{
    public const int BoundaryValueSize = Keccak.Size + PreambleLength;
    private const byte Byte0 = 0;
    private const byte Byte1 = 0;
    private const int PreambleLength = 2;

    public static Span<byte> WriteBoundaryValue(in Keccak value, in Span<byte> payload)
    {
        payload[0] = Byte0;
        payload[1] = Byte1;
        value.Span.CopyTo(payload[PreambleLength..]);

        return payload.Slice(0, BoundaryValueSize);
    }

    public static bool CanBeBoundaryLeaf(in Node.Leaf leaf)
    {
        return leaf.Path.HasOnlyZeroes();
    }

    public static bool IsBoundaryValue(in ReadOnlySpan<byte> value)
    {
        return value.Length == BoundaryValueSize && value[0] == Byte0 && value[1] == Byte1;
    }

    public static bool TryGetBoundaryValue(in ReadOnlySpan<byte> value, out Keccak keccak)
    {
        if (IsBoundaryValue(value))
        {
            keccak = default;
            value[PreambleLength..].CopyTo(keccak.BytesAsSpan);
            return true;
        }

        keccak = default;
        return false;
    }

    public static NibblePath CreateKey(scoped in NibblePath path, Span<byte> bytes)
    {
        var fillWithZeroes = NibblePath.FromKey(Keccak.Zero).SliceFrom(path.Length);
        return path.Append(fillWithZeroes, bytes);
    }
}