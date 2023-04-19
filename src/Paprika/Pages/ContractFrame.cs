using System.Runtime.InteropServices;
using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika.Pages;

/// <summary>
/// Account frame represents a memory chunk big enough to store information about one Ethereum account.
/// </summary>
/// <remarks>
/// Maximum RLP serialized account for foreseeable future size is 85 bytes.
/// This works both for EOA and Contracts.
/// The following test was used to prove it https://github.com/NethermindEth/nethermind/blob/paprika-playground/src/Nethermind/Nethermind.Core.Test/Encoding/PaprikaTests.cs
/// This value will not changed within a few years. 
/// The alignment is desirable. Some space should be left for additional data. A good aligned value is **96 bytes** (16 * 6). It leaves **11 bytes** for additional data.
/// 1. length of the RLP (1 byte)
/// 2. 10 additional bytes
/// 5. additionally, leafs have a part of a key that is **32 bytes** which gives in total **96 bytes** + **32 bytes** = **128 bytes**
/// 6. **128 bytes** is an upper boundary for a bucket of key→value
/// 7. this **128 bytes** will be called a **frame** (slot is a reserved keyword for storage).
/// 8. a single 4kb page with no management overhead could host up to 32 frames
/// </remarks>
///
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct ContractFrame
{
    public const int Size = FrameHeader.FrameUnit * 2;

    private const int StorageAndCodeHashOffset = 64;

    [FieldOffset(FrameHeader.Offset)]
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

/// <summary>
/// Provides a frame for EOA, which stores only <see cref="Nonce"/> and <see cref="Balance"/>.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct EOAFrame
{
    public const int Size = FrameHeader.FrameUnit;

    [FieldOffset(FrameHeader.Offset)]
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

[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct FrameHeader
{
    public const int Size = 4;

    public const int Offset = 0;

    /// <summary>
    /// The number of bytes, the granularity of a measure of a frame size. 
    /// </summary>
    public const int FrameUnit = 64;

    [FieldOffset(0)]
    public FrameType Type;

    /// <summary>
    /// What is the next linked frame in this bucket.
    /// </summary>
    [FieldOffset(2)]
    public byte NextFrame;

    /// <summary>
    /// How many frames the given bucket uses.
    /// </summary>
    [FieldOffset(3)]
    public byte FrameSizeInUnits;

    public FrameHeader Build(FrameType type, byte frameSizeInUnits, FrameHeader previous) =>
        new()
        {
            Type = type,
            FrameSizeInUnits = frameSizeInUnits,
            NextFrame = previous.NextFrame
        };
}

public enum FrameType : byte
{
    /// <summary>
    /// Identifies <see cref="EOAFrame"/>.
    /// </summary>
    EOA = 1,

    /// <summary>
    /// Identifies <see cref="EOAFrame"/>.
    /// </summary>
    Contract = 2,
}

