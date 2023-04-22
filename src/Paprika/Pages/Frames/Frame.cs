using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Pages.Frames;

/// <summary>
/// Provides a wrapping for a <see cref="byte"/> based index of a <see cref="IFrame"/> on the page,
/// so that 0 can be used as a value and is different from the null value. 
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public readonly struct Frame
{
    public const int Size = 8;

    public static Span<byte> Read(ref Frame frame, out byte next)
    {
        var header = Unsafe.As<Frame, Header>(ref frame);
        next = header.Next;

        return CreateByteSpan(ref frame, header);
    }

    public static void Write(ref Frame frame, Span<byte> bytes, byte next)
    {
        var length = GetFrameCount(bytes);

        ref var header = ref Unsafe.As<Frame, Header>(ref frame);

        header.Length = length;
        header.Next = next;

        var dest = CreateByteSpan(ref frame, header);
        bytes.CopyTo(dest);
    }

    private static Span<byte> CreateByteSpan(ref Frame frame, Header header)
    {
        ref var bytes = ref Unsafe.Add(ref Unsafe.As<Frame, byte>(ref frame), Header.Size);
        return MemoryMarshal.CreateSpan(ref bytes, header.Length * Size - Header.Size);
    }

    /// <summary>
    /// Gets the number of frames needed to store <see cref="bytes"/> payload.
    /// </summary>
    public static byte GetFrameCount(Span<byte> bytes) => (byte)AlignToFrames(bytes.Length + Header.Size);

    private static int AlignToFrames(int length) => ((length + (Size - 1)) & -Size) / Size;

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Header
    {
        // ReSharper disable once MemberHidesStaticFromOuterClass
        public const int Size = 2;

        [FieldOffset(0)] public byte Length;

        [FieldOffset(1)] public byte Next;
    }
}