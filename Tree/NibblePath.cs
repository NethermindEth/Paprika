using System.Runtime.CompilerServices;

namespace Tree;

/// <summary>
/// Represents a nibble path in a way that makes it efficient for comparisons.
/// </summary>
/// <remarks>
/// The implementation diverges from the Ethereum encoding for extensions or leafs.
/// The divergence is to never perform bit shift of the whole path and always align to byte boundary.
/// If the path starts in on odd nibble, it will include
/// </remarks>
public readonly ref struct NibblePath
{
    private const int NibblePerByte = 2;
    private const int NibbleShift = 8 / NibblePerByte;
    private const int NibbleMask = 15;
    
    private const int LengthShift = 1;
    private const int PreambleLength = 1;
    private const int OddBit = 1;

    private readonly ReadOnlySpan<byte> _span;
    private readonly byte _nibbleFrom;
    private readonly byte _length;

    public static NibblePath FromKey(ReadOnlySpan<byte> key, int nibbleFrom)
    {
        var count = key.Length * NibblePerByte;
        return new NibblePath(key, nibbleFrom, count - nibbleFrom);
    }

    private NibblePath(ReadOnlySpan<byte> key, int nibbleFrom, int length)
    {
        _span = key;
        _nibbleFrom = (byte)nibbleFrom;
        _length = (byte)length;
    }

    public int MaxLength => _span.Length + 2;

    public Span<byte> WriteTo(Span<byte> destination)
    {
        var odd = _nibbleFrom & OddBit;
        var byteStart = _nibbleFrom / 2;
        var lenght = GetSpanLength(_length, odd);

        destination[0] = (byte)(odd | (_length << LengthShift));
        _span.Slice(byteStart, lenght).CopyTo(destination.Slice(PreambleLength));

        return destination.Slice(lenght + PreambleLength);
    }

    private static int GetSpanLength(byte length, int odd) => (length + 1 + odd) / 2;

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out NibblePath nibblePath)
    {
        var b = source[0];

        var odd = OddBit & b;
        var length = (byte)(b >> LengthShift);

        nibblePath = new NibblePath(source.Slice(1), odd, length);

        return source.Slice(PreambleLength + GetSpanLength(length, odd));
    }

    public bool Equals(NibblePath other)
    {
        if (other._length != _length || (other._nibbleFrom & OddBit) != (_nibbleFrom & OddBit))
            return false;

        ref var a = ref Unsafe.AsRef(_span[_nibbleFrom / 2]);
        ref var b = ref Unsafe.AsRef(other._span[other._nibbleFrom / 2]);

        var length = _length;

        var isOdd = (other._nibbleFrom & OddBit) == OddBit;
        if (isOdd)
        {
            if ((a >> NibbleShift) != (b >> NibbleShift))
            {
                return false;
            }

            // move by 1
            a = ref Unsafe.Add(ref a, 1);
            b = ref Unsafe.Add(ref b, 1);
            length -= 1;
        }

        // odd or not, ready to make huge jumps

        // long
        const int longJump = 8;
        while (length > longJump * NibblePerByte)
        {
            var u1 = Unsafe.ReadUnaligned<ulong>(ref a);
            var u2 = Unsafe.ReadUnaligned<ulong>(ref b);

            if (u1 != u2)
            {
                return false;
            }

            a = ref Unsafe.Add(ref a, longJump);
            b = ref Unsafe.Add(ref b, longJump);

            length -= longJump * NibblePerByte;
        }

        // uint
        const int intJump = 4;
        if (length > intJump * NibblePerByte)
        {
            var u1 = Unsafe.ReadUnaligned<uint>(ref a);
            var u2 = Unsafe.ReadUnaligned<uint>(ref b);

            if (u1 != u2)
            {
                return false;
            }

            a = ref Unsafe.Add(ref a, intJump);
            b = ref Unsafe.Add(ref b, intJump);

            length -= intJump * NibblePerByte;
        }

        // length must be less than 8 nibbles (4 bytes)
        // scan through
        const int byteJump = 1;
        while (length > byteJump * NibblePerByte)
        {
            if (a != b)
                return false;

            a = ref Unsafe.Add(ref a, byteJump);
            b = ref Unsafe.Add(ref b, byteJump);

            length -= byteJump * NibblePerByte;
        }

        return length == 0 || (a & NibbleMask) == (b & NibbleMask);
    }
}