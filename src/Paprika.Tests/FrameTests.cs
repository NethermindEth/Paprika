using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages.Frames;

namespace Paprika.Tests;

public class FrameTests
{
    private const int MaxFrames = 16;
    private static readonly FrameIndex Next = FrameIndex.FromIndex(23);

    [TestCase(new byte[0], TestName = "Empty Span")]
    [TestCase(new byte[] { 13 }, TestName = "Single byte")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6 }, TestName = "Full frame")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, TestName = "2 frames")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 }, TestName = "2 full frames")]
    [TestCase(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, TestName = "3 frames")]
    public void Write_Read(byte[] bytes)
    {
        Span<Frame> frames = stackalloc Frame[MaxFrames];
        var pool = new Frame.Pool();

        pool.TryWrite(bytes, Next, frames, out var writtenTo).Should().BeTrue();

        var read = pool.Read(writtenTo, frames, out var next);

        Assert.AreEqual(Next, next);

        read.StartsWith(bytes).Should()
            .BeTrue("There can be more bytes in the frame, but they should start with original");
    }

    [Test]
    public void Write_Release_Write()
    {
        var bytes0 = new byte[] { 13 };
        var bytes1 = new byte[] { 17 };
        var bytes2 = new byte[] { 23 };

        Span<Frame> frames = stackalloc Frame[2];
        var pool = new Frame.Pool();

        pool.TryWrite(bytes0, FrameIndex.Null, frames, out var writtenTo0).Should().BeTrue();
        pool.TryWrite(bytes1, writtenTo0, frames, out var writtenTo1).Should().BeTrue();

        // TODO: how to check links?
        pool.Release(writtenTo1, frames);

        pool.TryWrite(bytes2, writtenTo0, frames, out var writtenTo2).Should().BeTrue();

        writtenTo1.Raw.Should().Be(writtenTo2.Raw);

        var read = pool.Read(writtenTo2, frames, out var next);

        Assert.AreEqual(writtenTo0, next);

        read.StartsWith(bytes2).Should()
            .BeTrue("There can be more bytes in the frame, but they should start with original");
    }
}