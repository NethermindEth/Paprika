using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Pages.Frames;

/// <summary>
/// Provides a frame for the contract, which stores
/// <see cref="Nonce"/> and <see cref="Balance"/>, <see cref="CodeHash"/> and <see cref="StorageRoot"/>.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct ContractFrame : IFrame
{
    public const int SizeInUnits = 2;
    public const int Size = IFrame.FrameUnit * SizeInUnits;

    private const int StorageAndCodeHashOffset = 64;

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

    [FieldOffset(StorageAndCodeHashOffset)]
    public Keccak StorageRoot;

    [FieldOffset(StorageAndCodeHashOffset + Keccak.Size)]
    public Keccak CodeHash;
}
