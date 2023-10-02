using System.Diagnostics;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

public readonly ref struct StoreKey
{
    public readonly ReadOnlySpan<byte> Payload;

    public StoreKey(ReadOnlySpan<byte> payload)
    {
        Payload = payload;
    }

    /// <summary>
    /// See <see cref="DataType"/> values.
    /// </summary>
    private const byte TypeMask = 0b0000_0111;
    private const byte LastByteMask = 0b1111_0000;
    private const byte OddNibbles = 0b0000_1000;
    private const byte OddNibblesShift = 3;

    public const int MaxByteSize = Keccak.Size + Keccak.Size + 1;

    public static int GetMaxByteSize(in Key key)
    {
        if (key.IsAccountCompressed)
        {
            return key.Path.Length / 2 + GetNibblePathLength(key.StoragePath);
        }

        return key.Path.Length == NibblePath.KeccakNibbleCount
            ? Keccak.Size + GetNibblePathLength(key.StoragePath)
            : GetNibblePathLength(key.Path);

        static int GetNibblePathLength(in NibblePath path)
        {
            return path.Length % 2 == 0
                ? path.Length / 2 + 1 // path is even, requires one more byte                  
                : (path.Length + 1) / 2;
        }
    }

    public static StoreKey Encode(scoped in Key key, Span<byte> destination)
    {
        int written;
        if (key.Path.Length == NibblePath.KeccakNibbleCount || key.IsAccountCompressed)
        {
            var raw = key.Path.RawSpan;
            raw.CopyTo(destination);
            written = raw.Length;

            written += Write(key.StoragePath, destination.Slice(raw.Length), key.Type);
            return new StoreKey(destination.Slice(0, written));
        }

        written = Write(key.Path, destination, key.Type);

        return new StoreKey(destination.Slice(0, written));

        static int Write(in NibblePath path, Span<byte> destination, DataType type)
        {
            var raw = path.RawSpan;
            raw.CopyTo(destination);

            var t = (byte)type;
            Debug.Assert(t <= TypeMask, "Type should be selectable by mask");

            if (path.Length % 2 == 0)
            {
                // even path, write last byte
                destination[raw.Length] = t;
                return raw.Length + 1;
            }

            ref var last = ref destination[raw.Length - 1];
            last = (byte)((last & LastByteMask) | OddNibbles | t);
            return raw.Length;
        }
    }

    public byte GetNibbleAt(int offset)
    {
        Debug.Assert(offset < NibbleCount);

        var b = Payload[offset / 2];
        var odd = offset & 1;
        return (byte)((b >> ((1 - odd) * NibblePath.NibbleShift)) & 0x0F);
    }

    public int NibbleCount => (Payload.Length - 1) * 2 + ((Payload[^1] & OddNibbles) >> OddNibblesShift);

    public StoreKey SliceTwoNibbles()
    {
        Debug.Assert(NibbleCount >= 2);
        return new StoreKey(Payload[1..]);
    }

    public DataType Type => (DataType)(Payload[^1] & TypeMask);
}