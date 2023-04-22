namespace Paprika.Pages.Frames;

/// <summary>
/// A frame represents a place in a Paprika <see cref="Page"/>
/// where Ethereum related data can be put.
/// Each type of a frame has a constant size but different types can have different sizes.
/// </summary>
/// <remarks>
/// Size considerations.
/// 
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
public interface IFrame
{
    /// <summary>
    /// The number of bytes, the granularity of a measure of a frame size.
    /// Every frame will be of size of N * <see cref="FrameUnit"/>.
    /// </summary>
    public const int FrameUnit = 64;

    /// <summary>
    /// The binary offset of the frame header, shared amongst all the frames.
    /// </summary>
    public const int HeaderOffset = 0;
}