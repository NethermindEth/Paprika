using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class PageBufferTests
{
    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        Span<byte> span = stackalloc byte[PageBuffer.MixSize];
        var buffer = new PageBuffer(span);

        Span<byte> key = stackalloc byte[4];
        new Random(13).NextBytes(key);

        Span<byte> data = stackalloc byte[1] { 23 };

        Index<ushort> prev = Index<ushort>.Null;
        buffer.TrySet(key, data, ref prev).Should().BeTrue();

        buffer.TryGet(key, out var retrieved, prev).Should().BeTrue();
        data.SequenceEqual(retrieved).Should().BeTrue();

        buffer.Delete(key, prev).Should().BeTrue("Should find and delete entry");
        buffer.TryGet(key, out _, prev).Should().BeFalse("The entry shall no longer exist");
    }
}