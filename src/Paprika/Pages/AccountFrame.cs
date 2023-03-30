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
// TODO: optimize and provide a mechanism so that a frame can host an EOA data within 64bits only
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct AccountFrame
{
    public const int Size = 128;
    private const int StorageAndCodeHashOffset = 64;

    [FieldOffset(0)]
    public Keccak Key;

    /// <summary>
    /// This field may be used to hold any needed flags, for example the type of the account frame.
    /// </summary>
    [FieldOffset(Keccak.Size)]
    public ulong ReservedForFutureUse;

    /// <summary>
    /// The jump to the next frame in the same bucket.
    /// </summary>
    [FieldOffset(Keccak.Size + sizeof(ulong))]
    public DbAddress Next;

    [FieldOffset(Keccak.Size + sizeof(ulong) + DbAddress.Size)]
    public uint Nonce;

    [FieldOffset(Keccak.Size + sizeof(ulong) + DbAddress.Size + sizeof(uint))]
    public UInt128 Balance;

    // the second half, used only for contracts
    [FieldOffset(StorageAndCodeHashOffset)]
    public Keccak StorageRoot;

    [FieldOffset(StorageAndCodeHashOffset + Keccak.Size)]
    public Keccak CodeHash;
}