using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Pages.Frames;

/// <summary>
/// Provides a frame for EOA, which stores only <see cref="Nonce"/> and <see cref="Balance"/>.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct EOAFrame : IFrame
{
    public const int SizeInUnits = 1;
    public const int Size = SizeInUnits * IFrame.FrameUnit;

    [FieldOffset(IFrame.HeaderOffset)]
    public FrameHeader Header;

    [FieldOffset(FrameHeader.Size)]
    public Keccak Key;

    [FieldOffset(FrameHeader.Size + Keccak.Size)]
    public uint Nonce;

    [FieldOffset(FrameHeader.Size + Keccak.Size + sizeof(uint))]
    public UInt128 Balance;

    // 8 bytes left unused at the end
    // [FieldOffset(FrameHeader.Size + Keccak.Size + sizeof(uint) + 16)]
    // public ulong Unused;
}