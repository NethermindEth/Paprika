using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Data;

public static class BitVector
{
    public interface IBitVector
    {
        public static abstract ushort Count { get; }
    }

    private const int BitsPerByte = 8;
    private const int Shift = 3;
    private const int Mask = 7;

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Of1024 : IBitVector
    {
        public const int Size = Count / BitsPerByte;
        public const ushort Count = 1024;

        [FieldOffset(0)] private byte _start;

        public bool this[int bit]
        {
            get => Get(ref _start, bit);
            set => Set(ref _start, bit, value);
        }

        public ushort FirstNotSet => FirstNotSet(this);
        
        public bool HasEmptyBits => HasEmptyBits(this);
        
        static ushort IBitVector.Count => Count;
    }
    
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Of512 : IBitVector
    {
        public const int Size = Count / BitsPerByte;
        public const ushort Count = 512;

        [FieldOffset(0)] private byte _start;

        public bool this[int bit]
        {
            get => Get(ref _start, bit);
            set => Set(ref _start, bit, value);
        }

        public ushort FirstNotSet => FirstNotSet(this);
        
        public bool HasEmptyBits => HasEmptyBits(this);
        
        static ushort IBitVector.Count => Count;
    }

    public static bool HasEmptyBits<TBitVector>(in TBitVector vector)
        where TBitVector : struct, IBitVector
    {
        return FirstNotSet(vector) != TBitVector.Count;
    }
    
    public static ushort FirstNotSet<TBitVector>(in TBitVector vector)
        where TBitVector : struct, IBitVector
    {
            var size = TBitVector.Count / BitsPerByte;
            const int chunk = sizeof(ulong);
            var count = size / chunk;

            for (var i = 0; i < count; i++)
            {
                var skip = i * chunk;
                ref var b = ref Unsafe.As<TBitVector, byte>(ref Unsafe.AsRef(in vector));
                
                var v = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref b, skip));
                if (BitOperations.PopCount(v) != chunk * BitsPerByte)
                {
                    return (ushort)(skip * BitsPerByte + BitOperations.TrailingZeroCount(~v));
                }
            }

            return TBitVector.Count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Get(ref byte b, int bit)
    {
        var at = bit >> Shift;
        var selector = 1 << (bit & Mask);
        return (Unsafe.Add(ref b, at) & selector) == selector;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Set(ref byte b, int bit, bool value)
    {
        unchecked
        {
            var at = bit >> Shift;
            var selector = 1 << (bit & Mask);
            ref var @byte = ref Unsafe.Add(ref b, at);

            if (value)
            {
                @byte |= (byte)selector;
            }
            else
            {
                @byte &= (byte)~selector;
            }
        }
    }
}