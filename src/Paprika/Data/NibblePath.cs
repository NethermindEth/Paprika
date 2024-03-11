using System;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Data;

/// <summary>
/// Represents a nibble path in a way that makes it efficient for comparisons.
/// </summary>
/// <remarks>
/// The implementation diverges from the Ethereum encoding for extensions or leafs.
/// The divergence is to never perform bit shift of the whole path and always align to byte boundary.
/// If the path starts in on odd nibble, it will include one byte and use only its higher nibble.
/// </remarks>
public readonly ref struct NibblePath
{
    public const int MaxLengthValue = byte.MaxValue / 2 + 2;
    public const int NibblePerByte = 2;
    public const int NibbleShift = 8 / NibblePerByte;
    public const int NibbleMask = 15;

    private const int LengthShift = 1;
    private const int PreambleLength = 1;
    private const int OddBit = 1;

    public readonly byte Length;
    private readonly ref byte _span;
    private readonly byte _odd;

    public bool IsOdd => _odd == OddBit;

    public static NibblePath Empty => default;

    /// <summary>
    /// The byte length of an empty path.
    /// </summary>
    public const int EmptyEncodedLength = 1;

    public bool IsEmpty => Length == 0;

    public static NibblePath Parse(string hex)
    {
        var nibbles = new byte[(hex.Length + 1) / 2];
        var path = FromKey(nibbles).SliceTo(hex.Length);

        for (var i = 0; i < hex.Length; i++)
        {
            path.UnsafeSetAt(i, 0, byte.Parse(hex.AsSpan(i, 1), NumberStyles.HexNumber));
        }

        return path;
    }

    public static NibblePath FromKey(ReadOnlySpan<byte> key, int nibbleFrom = 0)
    {
        var count = key.Length * NibblePerByte;
        return new NibblePath(key, nibbleFrom, count - nibbleFrom);
    }

    /// <summary>
    /// Creates a nibble path from raw nibbles (a byte per nibble), using the <paramref name="workingSet"/> as the memory to use.
    /// </summary>
    public static NibblePath FromRawNibbles(ReadOnlySpan<byte> nibbles, Span<byte> workingSet)
    {
        var span = workingSet.Slice(0, (nibbles.Length + 1) / 2);
        var copy = new NibblePath(span, 0, nibbles.Length);

        for (int i = 0; i < nibbles.Length; i++)
        {
            copy.UnsafeSetAt(i, 0, nibbles[i]);
        }

        return copy;
    }

    /// <summary>
    /// Reuses the memory of this nibble path moving it to odd position.
    /// </summary>
    public NibblePath UnsafeMakeOdd(int odd)
    {
        if (odd == 0)
            return this;

        Debug.Assert(_odd == 0, "Should not be applied to odd");

        for (int i = Length; i > 0; i--)
        {
            UnsafeSetAt(i, 0, GetAt(i - 1));
        }

        return new NibblePath(ref _span, OddBit, Length);
    }

    /// <summary>
    /// Creates the nibble path from preamble and raw slice
    /// </summary>
    public static NibblePath FromRaw(byte preamble, ReadOnlySpan<byte> slice)
    {
        return new NibblePath(slice, preamble & OddBit, preamble >> LengthShift);
    }

    /// <summary>
    /// </summary>
    /// <param name="key"></param>
    /// <param name="nibbleFrom"></param>
    /// <returns>
    /// The Keccak needs to be "in" here, as otherwise a copy would be create and the ref
    /// would point to a garbage memory.
    /// </returns>
    public static NibblePath FromKey(in Keccak key, int nibbleFrom = 0)
    {
        var count = Keccak.Size * NibblePerByte;
        return new NibblePath(key.BytesAsSpan, nibbleFrom, count - nibbleFrom);
    }

    /// <summary>
    /// Returns the underlying payload as <see cref="Keccak"/>.
    /// It does it in an unsafe way and requires an external check whether it's possible.
    /// </summary>
    public ref Keccak UnsafeAsKeccak => ref Unsafe.As<byte, Keccak>(ref _span);

    private NibblePath(ReadOnlySpan<byte> key, int nibbleFrom, int length)
    {
        _span = ref Unsafe.Add(ref MemoryMarshal.GetReference(key), nibbleFrom / 2);
        _odd = (byte)(nibbleFrom & OddBit);
        Length = (byte)length;
    }

    private NibblePath(ref byte span, byte odd, byte length)
    {
        _span = ref span;
        _odd = odd;
        Length = length;
    }

    /// <summary>
    /// The estimate of the max length, used for stackalloc estimations.
    /// </summary>
    public int MaxByteLength => Length / 2 + 2;

    public const int KeccakNibbleCount = Keccak.Size * NibblePerByte;

    public const int FullKeccakByteLength = Keccak.Size + 2;

    /// <summary>
    /// Writes the nibble path into the destination.
    /// </summary>
    /// <param name="destination">The destination to write to.</param>
    /// <returns>The leftover that other writers can write to.</returns>
    public Span<byte> WriteToWithLeftover(Span<byte> destination)
    {
        var lenght = WriteImpl(destination);
        return destination.Slice(lenght);
    }

    /// <summary>
    /// Writes the nibbles to the destination.  
    /// </summary>
    /// <param name="destination"></param>
    /// <returns>The actual bytes written.</returns>
    public Span<byte> WriteTo(Span<byte> destination)
    {
        var length = WriteImpl(destination);
        return destination.Slice(0, length);
    }

    public byte RawPreamble => (byte)((_odd & OddBit) | (Length << LengthShift));

    private int WriteImpl(Span<byte> destination)
    {
        var odd = _odd & OddBit;
        var length = GetSpanLength(Length, _odd);

        destination[0] = (byte)(odd | (Length << LengthShift));

        MemoryMarshal.CreateSpan(ref _span, length).CopyTo(destination.Slice(PreambleLength));

        // clearing the oldest nibble, if needed
        // yes, it can be branch free
        if (((odd + Length) & 1) == 1)
        {
            ref var oldest = ref destination[length];
            oldest = (byte)(oldest & 0b1111_0000);
        }

        return length + PreambleLength;
    }

    /// <summary>
    /// Slices the beginning of the nibble path as <see cref="Span{T}.Slice(int)"/> does.
    /// </summary>
    public NibblePath SliceFrom(int start)
    {
        Debug.Assert(Length - start >= 0, "Path out of boundary");

        if (Length - start == 0)
            return Empty;

        return new(ref Unsafe.Add(ref _span, (_odd + start) / 2),
            (byte)((start & 1) ^ _odd), (byte)(Length - start));
    }

    /// <summary>
    /// Trims the end of the nibble path so that it gets to the specified length.
    /// </summary>
    public NibblePath SliceTo(int length)
    {
        Debug.Assert(length <= Length, "Cannot slice the NibblePath beyond its Length");
        return new NibblePath(ref _span, _odd, (byte)length);
    }

    public byte this[int nibble] => GetAt(nibble);

    public byte GetAt(int nibble) => (byte)((GetRefAt(nibble) >> GetShift(nibble)) & NibbleMask);

    private int GetShift(int nibble) => (1 - ((nibble + _odd) & OddBit)) * NibbleShift;

    /// <summary>
    /// Sets a <paramref name="value"/> of the nibble at the given <paramref name="nibble"/> location.
    /// This is unsafe. Use only for owned memory. 
    /// </summary>
    private void UnsafeSetAt(int nibble, byte countOdd, byte value)
    {
        ref var b = ref GetRefAt(nibble);
        var shift = GetShift(nibble + countOdd);
        var mask = NibbleMask << shift;

        b = (byte)((b & ~mask) | (value << shift));
    }

    private ref byte GetRefAt(int nibble) => ref Unsafe.Add(ref _span, (nibble + _odd) / 2);

    /// <summary>
    /// Appends a <paramref name="nibble"/> to the end of the path,
    /// using the <paramref name="workingSet"/> as the underlying memory for the new new <see cref="NibblePath"/>.
    /// </summary>
    /// <remarks>
    /// The copy is required as the original path can be based on the readonly memory.
    /// </remarks>
    /// <returns>The newly copied nibble path.</returns>
    public NibblePath AppendNibble(byte nibble, Span<byte> workingSet)
    {
        if (workingSet.Length < MaxByteLength)
        {
            throw new ArgumentException("Not enough memory to append");
        }

        // TODO: do a ref comparison with Unsafe, if the same, no need to copy!
        WriteTo(workingSet);

        var appended = new NibblePath(ref workingSet[PreambleLength], _odd, (byte)(Length + 1));
        appended.UnsafeSetAt(Length, 0, nibble);
        return appended;
    }

    /// <summary>
    /// Appends the <see cref="other"/> path using the <paramref name="workingSet"/> as the working memory.
    /// </summary>
    public NibblePath Append(scoped in NibblePath other, Span<byte> workingSet)
    {
        if (workingSet.Length <= MaxByteLength)
        {
            throw new ArgumentException("Not enough memory to append");
        }

        // TODO: do a ref comparison with Unsafe, if the same, no need to copy!
        WriteTo(workingSet);

        var appended = new NibblePath(ref workingSet[PreambleLength], _odd, (byte)(Length + other.Length));
        for (int i = 0; i < other.Length; i++)
        {
            appended.UnsafeSetAt(Length + i, 0, other[i]);
        }

        return appended;
    }

    /// <summary>
    /// Appends the <see cref="other1"/> and then <see cref="other2"/> path using the <paramref name="workingSet"/> as the working memory.
    /// </summary>
    public NibblePath Append(scoped in NibblePath other1, scoped in NibblePath other2, Span<byte> workingSet)
    {
        if (workingSet.Length <= MaxByteLength)
        {
            throw new ArgumentException("Not enough memory to append");
        }

        // TODO: do a ref comparison with Unsafe, if the same, no need to copy!
        WriteTo(workingSet);

        var appended = new NibblePath(ref workingSet[PreambleLength], _odd, (byte)(Length + other1.Length + other2.Length));

        for (var i = 0; i < other1.Length; i++)
        {
            appended.UnsafeSetAt(Length + i, 0, other1[i]);
        }

        for (var i = 0; i < other2.Length; i++)
        {
            appended.UnsafeSetAt(Length + other1.Length + i, 0, other2[i]);
        }

        return appended;
    }

    public byte FirstNibble => (byte)((_span >> ((1 - _odd) * NibbleShift)) & NibbleMask);

    private static int GetSpanLength(byte length, int odd) => (length + 1 + odd) / 2;

    /// <summary>
    /// Gets the raw underlying span behind the path, removing the odd encoding.
    /// </summary>
    public ReadOnlySpan<byte> RawSpan => MemoryMarshal.CreateSpan(ref _span, RawSpanLength);

    public int RawSpanLength => GetSpanLength(Length, _odd);

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out NibblePath nibblePath)
    {
        var b = source[0];

        var odd = OddBit & b;
        var length = (byte)(b >> LengthShift);

        nibblePath = new NibblePath(source.Slice(PreambleLength), odd, length);

        return source.Slice(PreambleLength + GetSpanLength(length, odd));
    }

    public static Span<byte> ReadFrom(Span<byte> source, out NibblePath nibblePath)
    {
        var b = source[0];

        var odd = OddBit & b;
        var length = (byte)(b >> LengthShift);

        nibblePath = new NibblePath(source.Slice(PreambleLength), odd, length);

        return source.Slice(PreambleLength + GetSpanLength(length, odd));
    }

    /// <summary>
    /// Reads the first byte of nibble of the path without decoding it fully.
    /// </summary>
    public static byte ReadFirstNibble(ReadOnlySpan<byte> source)
    {
        // Access the second byte first as then the range check is already done on the first.
        var doubleNibble = (int)source[PreambleLength];
        var metaData = source[0];
        var odd = OddBit & metaData;

        // inlined: GetShift
        return (byte)((doubleNibble >> ((1 - odd) * NibbleShift)) & NibbleMask);
    }

    public int FindFirstDifferentNibble(in NibblePath other)
    {
        var length = Math.Min(other.Length, Length);
        if (length == 0)
        {
            // special case, empty is different at zero
            return 0;
        }

        if (_odd == other._odd)
        {
            // The most common case in Trie.
            // As paths will start on the same level, the odd will be encoded same way for them.
            // This means that an unrolled version can be used.

            ref var left = ref _span;
            ref var right = ref other._span;

            var position = 0;
            var isOdd = (_odd & OddBit) != 0;
            if (isOdd)
            {
                // This means first byte is not a whole byte
                if ((left & NibbleMask) != (right & NibbleMask))
                {
                    // First nibble differs
                    return 0;
                }

                // Equal so start comparing at next byte
                position = 1;
            }

            // Byte length is half of the nibble length
            var byteLength = length / 2;
            if (!isOdd && ((length & 1) > 0))
            {
                // If not isOdd, but the length is odd, then we need to add one more byte
                byteLength += 1;
            }

            var leftSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref left, position), byteLength);
            var rightSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref right, position), byteLength);
            var divergence = leftSpan.CommonPrefixLength(rightSpan);

            position += divergence * 2;
            if (divergence == leftSpan.Length)
            {
                // Remove the extra nibble that made it up to a full byte, if added.
                return Math.Min(length, position);
            }

            // Check which nibble is different
            if ((leftSpan[divergence] & 0xf0) == (rightSpan[divergence] & 0xf0))
            {
                // Are equal, so the next nibble is the one that differs
                return position + 1;
            }

            return position;
        }

        return Fallback(in this, in other, length);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static int Fallback(in NibblePath @this, in NibblePath other, int length)
        {
            // fallback, the slow path version to make the method work in any case
            int i = 0;
            for (; i < length; i++)
            {
                if (@this.GetAt(i) != other.GetAt(i))
                {
                    return i;
                }
            }

            return length;
        }
    }

    const int HexPreambleLength = 1;
    public int HexEncodedLength => Length / NibblePerByte + HexPreambleLength;

    private const byte OddFlag = 0x10;
    private const byte LeafFlag = 0x20;

    public void HexEncode(Span<byte> destination, bool isLeaf)
    {
        destination[0] = (byte)(isLeaf ? LeafFlag : 0x000);

        // This is the usual fast path for leaves, as they are aligned with oddity and length.
        // length: odd, odd: 1
        // length: even, odd: 0
        if ((Length & OddBit) == _odd)
        {
            if (_odd == OddBit)
            {
                // store odd
                destination[0] += (byte)(OddFlag + (_span & NibbleMask));
            }

            // copy the rest as is
            MemoryMarshal.CreateSpan(ref Unsafe.Add(ref _span, _odd), Length / 2)
                .CopyTo(destination.Slice(HexPreambleLength));

            return;
        }

        // this cases should happen only on extensions, as leafs are aligned to the end of the key.
        ref var b = ref _span;

        // length: even, odd: 1
        if (_odd == OddBit)
        {
            // the length is even, no need to amend destination[0]
            for (var i = 0; i < Length / 2; i++)
            {
                destination[i + 1] = (byte)(((b & NibbleMask) << NibbleShift) |
                                            ((Unsafe.Add(ref b, 1) >> NibbleShift) & NibbleMask));
                b = ref Unsafe.Add(ref b, 1);
            }

            return;
        }

        // length: odd, odd: 0
        if ((Length & OddBit) == OddBit)
        {
            destination[0] += (byte)(OddFlag + ((b >> NibbleShift) & NibbleMask));

            // the length is even, no need to amend destination[0]
            for (var i = 0; i < Length / 2; i++)
            {
                destination[i + 1] = (byte)(((b & NibbleMask) << NibbleShift) |
                                            ((Unsafe.Add(ref b, 1) >> NibbleShift) & NibbleMask));
                b = ref Unsafe.Add(ref b, 1);
            }

            return;
        }


        throw new Exception("WRONG!");
    }

    public override string ToString()
    {
        if (Length == 0)
            return "";

        Span<char> path = stackalloc char[Length];
        ref var ch = ref path[0];

        for (int i = _odd; i < Length + _odd; i++)
        {
            var b = Unsafe.Add(ref _span, i / 2);
            var nibble = (b >> ((1 - (i & OddBit)) * NibbleShift)) & NibbleMask;

            ch = Hex[nibble];
            ch = ref Unsafe.Add(ref ch, 1);
        }

        return new string(path);
    }

    private static readonly char[] Hex = "0123456789ABCDEF".ToArray();

    public bool EndsWith(in NibblePath other) => SliceFrom(Length - other.Length).Equals(other);

    public bool Equals(in NibblePath other)
    {
        if (other.Length != Length || (other._odd & OddBit) != (_odd & OddBit))
            return false;

        return FindFirstDifferentNibble(other) == Length;
    }

    public override int GetHashCode()
    {
        if (Length <= 1)
        {
            // for a single nibble path, make it different from empty.
            return Length == 0 ? 0 : 1 << GetAt(0);
        }

        unchecked
        {
            ref var span = ref _span;

            uint hash = (uint)Length << 24;
            nuint length = Length;

            if (_odd == OddBit)
            {
                // mix in first half
                hash |= (uint)(_span & 0x0F) << 20;
                span = ref Unsafe.Add(ref span, 1);
                length -= 1;
            }

            if (length % 2 == 1)
            {
                // mix in
                hash |= (uint)GetAt((int)length - 1) << 16;
                length -= 1;
            }

            Debug.Assert(length % 2 == 0, "Length should be even here");

            length /= 2; // make it byte

            // 8 bytes
            if (length >= sizeof(long))
            {
                nuint offset = 0;
                nuint longLoop = length - sizeof(long);
                if (longLoop != 0)
                {
                    do
                    {
                        hash = BitOperations.Crc32C(hash,
                            Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref span, offset)));
                        offset += sizeof(long);
                    } while (longLoop > offset);
                }

                // Do final hash as sizeof(long) from end rather than start
                hash = BitOperations.Crc32C(hash, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref span, longLoop)));

                return (int)hash;
            }

            // 4 bytes
            if (length >= sizeof(int))
            {
                hash = BitOperations.Crc32C(hash, Unsafe.ReadUnaligned<uint>(ref span));
                length -= sizeof(int);
                if (length > 0)
                {
                    // Do final hash as sizeof(long) from end rather than start
                    hash = BitOperations.Crc32C(hash, Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref span, length)));
                }

                return (int)hash;
            }

            // 2 bytes
            if (length >= sizeof(short))
            {
                hash = BitOperations.Crc32C(hash, Unsafe.ReadUnaligned<ushort>(ref span));
                length -= sizeof(short);
                if (length > 0)
                {
                    // Do final hash as sizeof(long) from end rather than start
                    hash = BitOperations.Crc32C(hash, Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref span, length)));
                }

                return (int)hash;
            }

            // 1 byte
            return (int)BitOperations.Crc32C(hash, span);
        }
    }

    public bool HasOnlyZeroes()
    {
        // TODO: optimize
        for (var i = 0; i < Length; i++)
        {
            if (GetAt(i) != 0)
            {
                return false;
            }
        }

        return true;
    }
}