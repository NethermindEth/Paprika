namespace Tree;

public static class Id
{
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

    public static (int position, int length, int file) Decode(long id) =>
        ((int)((id & PositionMask) >> PositionShift),
            (int)((id & LengthMask) >> LengthShift),
            (int)((id & FileMask) >> FileShift));

    public static long Encode(int position, int length, int file) =>
        ((long)position << PositionShift) | ((long)length << LengthShift) | ((long)file << FileShift);
}