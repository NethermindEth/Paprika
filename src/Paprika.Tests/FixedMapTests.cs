using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class FixedMapTests
{
    private static ReadOnlySpan<byte> Key0 => new byte[] { 1, 2, 3, 5 };
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Key1 => new byte[] { 7, 11, 13, 17 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };
    private static ReadOnlySpan<byte> Key2 => new byte[] { 19, 21, 23, 29 };
    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };


    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        Span<byte> span = stackalloc byte[FixedMap.MixSize];
        var buffer = new FixedMap(span);

        buffer.TrySet(Key0, Data0).Should().BeTrue();

        buffer.TryGet(Key0, out var retrieved).Should().BeTrue();
        Data0.SequenceEqual(retrieved).Should().BeTrue();

        buffer.Delete(Key0).Should().BeTrue("Should find and delete entry");
        buffer.TryGet(Key0, out _).Should().BeFalse("The entry shall no longer exist");

        // should be ready to accept some data again

        buffer.TrySet(Key1, Data1).Should().BeTrue("Should have memory after previous delete");

        buffer.TryGet(Key1, out retrieved).Should().BeTrue();
        Data1.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Defragment_when_no_more_space()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[40];
        var buffer = new FixedMap(span);

        buffer.TrySet(Key0, Data0).Should().BeTrue();
        buffer.TrySet(Key1, Data1).Should().BeTrue();

        buffer.Delete(Key0).Should().BeTrue();

        buffer.TrySet(Key2, Data2).Should().BeTrue("Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        buffer.TryGet(Key0, out var retrieved).Should().BeFalse();

        buffer.TryGet(Key1, out retrieved).Should().BeTrue();
        Data1.SequenceEqual(retrieved).Should().BeTrue();

        buffer.TryGet(Key2, out retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var buffer = new FixedMap(span);

        buffer.TrySet(Key1, Data1).Should().BeTrue();
        buffer.TrySet(Key1, Data2).Should().BeTrue();

        buffer.TryGet(Key1, out var retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Update_in_resize()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var buffer = new FixedMap(span);

        buffer.TrySet(Key0, Data0).Should().BeTrue();
        buffer.TrySet(Key0, Data2).Should().BeTrue();

        buffer.TryGet(Key0, out var retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }
}