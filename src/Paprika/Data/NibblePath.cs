using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    /// <summary>
    /// An array of singles, that can be used to create a path of length 1, both odd and even.
    /// Used by <see cref="Single"/>.
    /// </summary>
    private static ReadOnlySpan<byte> Singles => new byte[]
    {
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF
    };

    /// <summary>
    /// Exposes <see cref="Singles"/> for tests only.
    /// </summary>
    public static ReadOnlySpan<byte> SinglesForTests => Singles;

    /// <summary>
    /// Creates a <see cref="NibblePath"/> with length of 1.
    /// </summary>
    /// <param name="nibble">The nibble that should be in the path.</param>
    /// <param name="odd">The oddity.</param>
    /// <returns>The path</returns>
    /// <remarks>
    /// Highly optimized, branch-less, just a few moves and adds.
    /// </remarks>
    public static NibblePath Single(byte nibble, int odd)
    {
        Debug.Assert(nibble <= NibbleMask, "Nibble breached the value");
        Debug.Assert(odd <= 1, "Odd should be 1 or 0");

        ref var singles = ref Unsafe.AsRef(ref MemoryMarshal.GetReference(Singles));
        return new NibblePath(ref Unsafe.Add(ref singles, nibble), (byte)odd, 1);
    }

    public const int MaxLengthValue = byte.MaxValue / 2 + 2;
    private const int NibblePerByte = 2;
    public const int NibbleShift = 8 / NibblePerByte;
    public const int NibbleMask = 15;

    private const int LengthShift = 1;
    private const int PreambleLength = 1;
    private const int OddBit = 1;

    private readonly ref byte _span;
    private readonly byte _odd;
    public readonly byte Length;

    public bool IsOdd => _odd == OddBit;
    public int Oddity => _odd;

    public static NibblePath Empty => default;

    public bool IsEmpty => Length == 0;

    public static NibblePath Parse(string hex)
    {
        var nibbles = new byte[(hex.Length + 1) / 2];
        var path = FromKey(nibbles).SliceTo(hex.Length);

        for (var i = 0; i < hex.Length; i++)
        {
            path.UnsafeSetAt(i, byte.Parse(hex.AsSpan(i, 1), NumberStyles.HexNumber));
        }

        return path;
    }

    public static NibblePath FromKey(ReadOnlySpan<byte> key, int nibbleFrom = 0)
    {
        var count = key.Length * NibblePerByte;
        return new NibblePath(key, nibbleFrom, count - nibbleFrom);
    }

    public static NibblePath FromKey(ReadOnlySpan<byte> key, int nibbleFrom, int length)
    {
        return new NibblePath(key, nibbleFrom, length);
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
            copy.UnsafeSetAt(i, nibbles[i]);
        }

        return copy;
    }

    public ref byte UnsafeSpan => ref _span;

    /// <summary>
    /// Reuses the memory of this nibble path moving it to odd position.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void UnsafeMakeOdd()
    {
        Debug.Assert(_odd == 0, "Should not be applied to odd");

        var i = (int)Length;
        if (i == 1)
        {
            _span = (byte)(_span >> NibbleShift);
            Unsafe.AsRef(in _odd) = OddBit;
        }
        else if (i <= 4)
        {
            var u = (uint)Unsafe.As<byte, ushort>(ref _span);
            var s = BinaryPrimitives.ReverseEndianness(u) >> NibbleShift;
            Unsafe.As<byte, ushort>(ref _span) = (ushort)BinaryPrimitives.ReverseEndianness(s);
            if (i == 4)
            {
                var overflow = ((s & 0xf000) >> (NibbleShift * 2));
                Unsafe.Add(ref _span, 2) = (byte)overflow;
            }

            Unsafe.AsRef(in _odd) = OddBit;
        }
        else
        {
            LargeUnsafeMakeOdd();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void LargeUnsafeMakeOdd()
    {
        for (var i = (int)Length; i > 0; i--)
        {
            UnsafeSetAt(i, GetAt(i - 1));
        }

        Unsafe.AsRef(in _odd) = OddBit;
    }

    /// <summary>
    /// </summary>
    /// <param name="key"></param>
    /// <param name="nibbleFrom"></param>
    /// <returns>
    /// The Keccak needs to be "in" here, as otherwise a copy would be created and the ref
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
        var length = WriteImpl(destination);
        return destination.Slice(length);
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
        var length = (int)Length;
        var spanLength = GetSpanLength(_odd, length);

        destination[0] = (byte)(odd | (length << LengthShift)); // The first byte is used for oddity and length
        // But, if we store length up to 6 in the preamble, we can directly store the remaining trimmed nibbles here.

        ref var destStart = ref Unsafe.Add(ref MemoryMarshal.GetReference(destination), PreambleLength);
        if (spanLength == sizeof(byte))
        {
            destStart = _span;
        }
        else if (spanLength == sizeof(ushort))
        {
            Unsafe.As<byte, ushort>(ref destStart) = Unsafe.As<byte, ushort>(ref _span);
        }
        else if (spanLength == sizeof(ushort) + sizeof(byte))
        {
            Unsafe.As<byte, ushort>(ref destStart) = Unsafe.As<byte, ushort>(ref _span);
            Unsafe.Add(ref destStart, sizeof(ushort)) = Unsafe.Add(ref _span, sizeof(ushort));
        }
        else if (!Unsafe.AreSame(ref _span, ref destStart))
        {
            MemoryMarshal.CreateSpan(ref _span, spanLength).CopyTo(destination.Slice(PreambleLength));
        }

        // If the path length is odd, clear the lower nibble of the last byte.
        // This ensures that truncated nibble paths (like 0xAB.SliceTo(1)) are stored unambiguously.
        // For instance, 0xAB.SliceTo(1) should result in 0xA0, so it can be distinguished from other paths.
        if (((odd + length) & 1) != 0)
        {
            ref var oldest = ref destination[spanLength];
            oldest = (byte)(oldest & 0b1111_0000);
        }

        return spanLength + PreambleLength;
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

    /// <summary>
    /// Trims the end of the nibble path so that it gets to the specified length.
    /// </summary>
    public NibblePath Slice(int start, int length)
    {
        Debug.Assert(start + length <= Length, "Cannot slice the NibblePath beyond its Length");
        return new(ref Unsafe.Add(ref _span, (_odd + start) / 2),
            (byte)((start & 1) ^ _odd), (byte)length);
    }

    public byte this[int nibble] => GetAt(nibble);

    public byte GetAt(int nibble) => (byte)((GetRefAt(nibble) >> GetShift(nibble)) & NibbleMask);

    private int GetShift(int nibble) => (1 - ((nibble + _odd) & OddBit)) * NibbleShift;

    /// <summary>
    /// Sets a <paramref name="value"/> of the nibble at the given <paramref name="nibble"/> location.
    /// This is unsafe. Use only for owned memory. 
    /// </summary>
    public void UnsafeSetAt(int nibble, byte value)
    {
        ref var b = ref GetRefAt(nibble);
        var shift = GetShift(nibble);
        var mask = NibbleMask << shift;

        b = (byte)((b & ~mask) | (value << shift));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            ThrowNotEnoughMemory();
        }

        // TODO: do a ref comparison with Unsafe, if the same, no need to copy!
        WriteTo(workingSet);

        var appended = new NibblePath(ref workingSet[PreambleLength], _odd, (byte)(Length + 1));
        appended.UnsafeSetAt(Length, nibble);
        return appended;
    }

    /// <summary>
    /// Appends the <see cref="other"/> path using the <paramref name="workingSet"/> as the working memory.
    /// </summary>
    public NibblePath Append(scoped in NibblePath other, Span<byte> workingSet)
    {
        if (workingSet.Length <= MaxByteLength)
        {
            ThrowNotEnoughMemory();
        }

        // TODO: do a ref comparison with Unsafe, if the same, no need to copy!
        WriteTo(workingSet);

        var length = (int)Length;
        var appended = new NibblePath(ref workingSet[PreambleLength], _odd, (byte)(length + other.Length));
        for (int i = 0; i < other.Length; i++)
        {
            appended.UnsafeSetAt(length + i, other[i]);
        }

        return appended;
    }

    /// <summary>
    /// Appends <see cref="other1"/> and then <see cref="other2"/> path using the <paramref name="workingSet"/> as the working memory.
    /// </summary>
    [SkipLocalsInit]
    public NibblePath Append(scoped in NibblePath other1, int hash, Span<byte> workingSet)
    {
        if (workingSet.Length <= MaxByteLength)
        {
            ThrowNotEnoughMemory();
        }

        WriteImpl(workingSet);

        var length = (int)Length;
        var appended = new NibblePath(ref workingSet[PreambleLength], _odd, (byte)(length + other1.Length + 2));

        if (other1.IsEmpty == false)
        {
            var alignment = (_odd + Length) ^ other1._odd;
            if ((alignment & OddBit) == 0)
            {
                // oddity aligned
                if (_odd == OddBit)
                {
                    // it's odd, set odd first
                    appended.UnsafeSetAt(length, other1[0]);
                }

                // unrolled version to copy byte by byte instead of nibbles
                var i = 0;
                for (; i < other1.Length - 1; i += 2)
                {
                    appended.GetRefAt(length + i + _odd) = other1.GetRefAt(i + _odd);
                }

                if (i < other1.Length)
                {
                    // it's odd, set odd first
                    appended.UnsafeSetAt(length + other1.Length - 1, other1[other1.Length - 1]);
                }
            }
            else
            {
                for (var i = 0; i < other1.Length; i++)
                {
                    appended.UnsafeSetAt(length + i, other1[i]);
                }
            }
        }

        var start = length + other1.Length;
        appended.UnsafeSetAt(start + 0, (byte)((hash & 0xf0) >> NibbleShift));
        appended.UnsafeSetAt(start + 1, (byte)(hash & 0xf));

        return appended;
    }

    public byte FirstNibble => (byte)((_span >> ((1 - _odd) * NibbleShift)) & NibbleMask);

    private static int GetSpanLength(int odd, int length) => (length + 1 + odd) / 2;

    /// <summary>
    /// Gets the raw underlying span behind the path, removing the odd encoding.
    /// </summary>
    public ReadOnlySpan<byte> RawSpan => MemoryMarshal.CreateSpan(ref _span, RawSpanLength);

    public int RawSpanLength => GetSpanLength(_odd, Length);

    public static ReadOnlySpan<byte> ReadFromWithLength(ReadOnlySpan<byte> source, int length, bool isOdd, out NibblePath nibblePath)
    {
        var odd = isOdd ? OddBit : 0;
        nibblePath = new NibblePath(source, odd, length);
        return source.Slice(GetSpanLength(odd, length));
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out NibblePath nibblePath)
    {
        var b = (int)source[0];

        var odd = OddBit & b;
        var length = (b >> LengthShift);

        source = source.Slice(PreambleLength);

        nibblePath = new NibblePath(source, odd, length);
        return source.Slice(GetSpanLength(odd, length));
    }

    public static Span<byte> ReadFromWithLength(Span<byte> source, int length, byte odd, out NibblePath nibblePath)
    {
        nibblePath = new NibblePath(source, odd, length);
        return source.Slice(GetSpanLength(odd, length));
    }

    public static Span<byte> ReadFrom(Span<byte> source, out NibblePath nibblePath)
    {
        var b = (int)source[0];

        var odd = OddBit & b;
        var length = (b >> LengthShift);

        source = source.Slice(PreambleLength);
        nibblePath = new NibblePath(source, odd, length);
        return source.Slice(GetSpanLength(odd, length));
    }

    public static bool TryReadFrom(Span<byte> source, in NibblePath expected, out Span<byte> leftover)
    {
        if (source[0] != expected.RawPreamble)
        {
            leftover = default;
            return false;
        }

        leftover = ReadFrom(source, out var actualKey);
        return actualKey.Equals(expected);
    }

    public static bool TryReadFromWithLength(Span<byte> source, in NibblePath expected, int length, byte isOdd, out Span<byte> leftover)
    {
        leftover = ReadFromWithLength(source, length, isOdd, out var actualKey);
        return actualKey.Equals(expected);
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

    private const int HexPreambleLength = 1;

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

    [DoesNotReturn]
    [StackTraceHidden]
    static void ThrowNotEnoughMemory()
    {
        throw new ArgumentException("Not enough memory to append");
    }
}
