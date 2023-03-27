﻿using System.Runtime.CompilerServices;

namespace Tree;

public static class Id
{
    public const int Size = sizeof(long);

    /// <summary>
    /// The number of bytes that data are written at, possibly smaller than <see cref="Size"/>.
    /// </summary>
    public const int NonZeroBytesSize = 7;

    public const long NonZeroBytesMask = FileMask | LengthMask | PositionMask;

    public const int MaxPosition = int.MaxValue;
    public const int MaxLength = short.MaxValue;
    public const int MaxFile = 0x0FF;

    private const int Byte = 8;

    // up to 2GB file size
    private const long PositionMask = 0xFFFF_FFFF;
    private const int PositionShift = 0;

    // up to 64k of length
    private const long LengthMask = 0xFFFF_0000_0000;
    private const int LengthShift = 4 * Byte;

    // up to 256 files
    private const long FileMask = 0x00FF_0000_0000_0000;
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

    public static bool IsSameFile(long id1, long id2) => ((id1 ^ id2) & FileMask) == 0;

    public static long Encode(int position, int length, int file) =>
        ((long)position << PositionShift) | ((long)length << LengthShift) | ((long)file << FileShift);

    public struct Decoded
    {
        public int Position;
        public ushort Length;
        public ushort File;
    }
}