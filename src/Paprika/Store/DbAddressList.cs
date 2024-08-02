using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Store;

/// <summary>
/// 
/// </summary>
/// <remarks>
/// <see cref="DbAddress"/> stored as 3.5 bytes to get addressable up to 1TB.
///
/// Stored as aa_aa_aa_ba_bb_bb_bb;
/// For little endian:
/// - even index should be read and intersected with <see cref="ValueMask"/>.
/// - odd index should be read and shifted right by <see cref="HalfByteShift"/>.
///         
/// </remarks>
public class DbAddressList
{
    public static readonly DbAddress Max = DbAddress.Page(ValueMask);

    private const int BytesPer2Addresses = 7;
    private const int Oddity = 1;
    private const uint ValueMask = 0x0F_FF_FF_FF;
    private const byte OneByteMaskLow = 0x0F;
    private const byte OneByteMaskHigh = 0xF0;

    private const int ShiftToOdd = 3;
    private const int HalfByteShift = 4;

    private static DbAddress Get(ref byte b, int index)
    {
        var odd = index & Oddity;
        var i = index >> 1;

        ref var slot = ref Unsafe.Add(ref b, i * BytesPer2Addresses);

        uint value;

        if (odd == Oddity)
        {
            // odd
            slot = ref Unsafe.Add(ref slot, ShiftToOdd);
            value = Unsafe.ReadUnaligned<uint>(ref slot) >> HalfByteShift;
        }
        else
        {
            // even
            value = Unsafe.ReadUnaligned<uint>(ref slot);
        }

        return DbAddress.Page(value & ValueMask);
    }

    private static void Set(ref byte b, int index, DbAddress value)
    {
        var odd = index & Oddity;
        var i = index >> 1;

        ref var slot = ref Unsafe.Add(ref b, i * BytesPer2Addresses);
        uint v;
        if (odd == Oddity)
        {
            // odd
            slot = ref Unsafe.Add(ref slot, ShiftToOdd);

            // Read the previous and set the lowest half-byte to the one from the slot. Shift the rest.
            v = (uint)(slot & OneByteMaskLow) | (value.Raw << HalfByteShift);
        }
        else
        {
            // even
            v = (uint)((Unsafe.Add(ref slot, 3) & OneByteMaskHigh) << 24) | value.Raw;
        }

        Unsafe.WriteUnaligned(ref slot, v);
    }

    public interface IDbAddressList
    {
        public DbAddress this[int index] { get; set; }
        public static abstract int Length { get; }
    }

    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    public struct Of16 : IDbAddressList
    {
        public const int Count = 16;
        public const int Size = Count / 2 * BytesPer2Addresses;

        private byte _b;

        public DbAddress this[int index]
        {
            get
            {
                Debug.Assert(index is >= 0 and < Count);
                return Get(ref _b, index);
            }
            set
            {
                Debug.Assert(index is >= 0 and < Count);
                Set(ref _b, index, value);
            }
        }

        public static int Length => Count;
    }

    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    public struct Of256 : IDbAddressList
    {
        public const int Count = 256;
        public const int Size = Count / 2 * BytesPer2Addresses;

        private byte _b;

        public DbAddress this[int index]
        {
            get
            {
                Debug.Assert(index is >= 0 and < Count);
                return Get(ref _b, index);
            }
            set
            {
                Debug.Assert(index is >= 0 and < Count);
                Set(ref _b, index, value);
            }
        }

        public static int Length => Count;
    }

    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    public struct Of1024 : IDbAddressList
    {
        public const int Count = 1024;
        public const int Size = Count / 2 * BytesPer2Addresses;

        private byte _b;

        public DbAddress this[int index]
        {
            get
            {
                Debug.Assert(index is >= 0 and < Count);
                return Get(ref _b, index);
            }
            set
            {
                Debug.Assert(index is >= 0 and < Count);
                Set(ref _b, index, value);
            }
        }

        public static int Length => Count;
    }
}