using System.Runtime.InteropServices;

namespace Paprika.Pages.Frames;

[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct FrameHeader
{
    public const int Size = 4;

    [FieldOffset(0)]
    public FrameType Type;

    [FieldOffset(1)]
    public byte FrameStart;

    /// <summary>
    /// How many frames the given bucket uses.
    /// </summary>
    [FieldOffset(2)]
    public byte FrameSizeInUnits;

    /// <summary>
    /// What is the next linked frame in this bucket.
    /// </summary>
    [FieldOffset(3)]
    public FrameIndex NextFrame;

    /// <summary>
    /// Builds the next header pointing to the previous one.
    /// </summary>
    public static FrameHeader BuildContract(FrameIndex current) =>
        new()
        {
            Type = FrameType.Contract,
            FrameSizeInUnits = ContractFrame.SizeInUnits,
            NextFrame = current
        };
}