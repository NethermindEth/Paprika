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

        Span<byte> key0 = stackalloc byte[4] { 1, 2, 3, 5 };
        Span<byte> data0 = stackalloc byte[1] { 23 };

        buffer.TrySet(key0, data0).Should().BeTrue();

        buffer.TryGet(key0, out var retrieved).Should().BeTrue();
        data0.SequenceEqual(retrieved).Should().BeTrue();

        buffer.Delete(key0).Should().BeTrue("Should find and delete entry");
        buffer.TryGet(key0, out _).Should().BeFalse("The entry shall no longer exist");

        // should be ready to accept some data again
        Span<byte> key1 = stackalloc byte[4] { 7, 11, 13, 17 };
        Span<byte> data1 = stackalloc byte[2] { 29, 31 };

        buffer.TrySet(key1, data1).Should().BeTrue("Should have memory after previous delete");

        buffer.TryGet(key1, out retrieved).Should().BeTrue();
        data1.SequenceEqual(retrieved).Should().BeTrue();
    }
}