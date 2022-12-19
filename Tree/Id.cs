using System.Runtime.CompilerServices;

namespace Tree;

public static class Id
{
    public const int Size = sizeof(long);
    
    public const int MaxPosition = int.MaxValue;
    public const int MaxLength = short.MaxValue;
    public const int MaxFile  = 0x0FFF;

    private const int Byte = 8;
    
    // up to 2GB file size
    private const long PositionMask = 0xFFFF_FFFF;
    private const int PositionShift = 0;
    
    // up to 64k of length
    private const long LengthMask = 0xFFFF_0000_0000;
    private const int LengthShift = 4 * Byte;
    
    // up to 4096 files
    private const long FileMask = 0x0FFF_0000_0000_0000;
    private const int FileShift = 6 * Byte;

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static Decoded Decode(long id)
    {
        Decoded result;

        result.Position = (int)((id & PositionMask) >> PositionShift);
        result.Length = (ushort)((id & LengthMask) >> LengthShift);
        result.File = (ushort)((id & FileMask) >> FileShift);

        return result;
    }
        

    public static long Encode(int position, int length, int file) =>
        ((long)position << PositionShift) | ((long)length << LengthShift) | ((long)file << FileShift);
    
    public struct Decoded
    {
        public int Position;
        public ushort Length;
        public ushort File;
    }
}