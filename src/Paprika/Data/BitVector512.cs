using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Data;

[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct BitVector1024
{
    public const int Count = 1024;
    public const int Size = Count / BitsPerByte;
    private const int BitsPerByte = 8;
    private const int Shift = 3;
    private const int Mask = 7;

    [FieldOffset(0)] private byte _start;

    public bool this[int bit]
    {
        get
        {
            var at = bit >> Shift;
            var selector = 1 << (bit & Mask);
            return (Unsafe.Add(ref _start, at) & selector) == selector;
        }
        set
        {
            unchecked
            {
                var at = bit >> Shift;
                var selector = 1 << (bit & Mask);
                ref var @byte = ref Unsafe.Add(ref _start, at);

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

    public bool HasEmptyBits => FirstNotSet != Count;

    public ushort FirstNotSet
    {
        get
        {
            const int chunk = sizeof(ulong);
            const int count = Size / chunk;

            for (var i = 0; i < count; i++)
            {
                var skip = i * chunk;
                var v = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref _start, skip));
                if (BitOperations.PopCount(v) != chunk * BitsPerByte)
                {
                    return (ushort)(skip * BitsPerByte + BitOperations.TrailingZeroCount(~v));
                }
            }

            return Count;
        }
    }
}