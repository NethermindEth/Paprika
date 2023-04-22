using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages.Frames;

namespace Paprika.Tests;

public class FrameTests
{
    private const int MaxFrames = 16;
    private const int Next = 23;

    [TestCase(new byte[0], 1, TestName = "Empty Span")]
    [TestCase(new byte[] { 13 }, 1, TestName = "Single byte")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6 }, 1, TestName = "Full frame")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, 2, TestName = "2 frames")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 }, 2, TestName = "2 full frames")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, 3, TestName = "3 frames")]
    public void Write_Read(byte[] bytes, byte expectedFrameCount)
    {
        Span<Frame> pool = stackalloc Frame[MaxFrames];
        ref var frame = ref pool[0];

        Frame.GetFrameCount(bytes).Should().Be(expectedFrameCount);

        Frame.Write(ref frame, bytes, Next);
        var read = Frame.Read(ref frame, out var next);

        Assert.AreEqual(Next, next);

        read.StartsWith(bytes).Should()
            .BeTrue("There can be more bytes in the frame, but they should start with original");
    }
}