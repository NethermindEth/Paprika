using System.Diagnostics;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

public readonly ref struct StoreKey
{
    public readonly ReadOnlySpan<byte> Data;

    private StoreKey(ReadOnlySpan<byte> data)
    {
        Data = data;
    }

    private const byte TypeMask = 0b0000_0011;
    private const byte LastByteMask = 0b1111_0000;
    private const byte OddNibbles = 0b0000_0100;
    private const byte OddNibblesShift = 2;

    public static int GetByteSize(in Key key)
    {
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

    public static StoreKey Encode(in Key key, Span<byte> destination)
    {
        int written;
        if (key.Path.Length == NibblePath.KeccakNibbleCount)
        {
            key.Path.RawSpan.CopyTo(destination);
            written = Keccak.Size;

            written += Write(key.StoragePath, destination.Slice(Keccak.Size), key.Type);
            return new StoreKey(destination.Slice(0, written));
        }
        else
        {
            written = Write(key.Path, destination, key.Type);
        }

        return new StoreKey(destination.Slice(0, written));

        static int Write(in NibblePath path, Span<byte> destination, DataType type)
        {
            var raw = path.RawSpan;
            raw.CopyTo(destination);

            if (path.Length % 2 == 0)
            {
                // even path, write last byte
                destination[raw.Length] = (byte)type;
                return raw.Length + 1;
            }

            ref var last = ref destination[raw.Length - 1];
            last = (byte)((last & LastByteMask) | OddNibbles | (byte)type);
            return raw.Length;
        }
    }

    public byte GetNibbleAt(int offset)
    {
        Debug.Assert(offset < NibbleCount);

        var b = Data[offset / 2];
        var odd = offset & 1;
        return (byte)((b >> ((1 - odd) * NibblePath.NibbleShift)) & 0x0F);
    }

    public int NibbleCount => (Data.Length - 1) * 2 + ((Data[^1] & OddNibbles) >> OddNibblesShift);

    public StoreKey SliceTwoNibbles()
    {
        Debug.Assert(NibbleCount >= 2);
        return new StoreKey(Data[1..]);
    }

    public DataType Type => (DataType)(Data[^1] & TypeMask);
}