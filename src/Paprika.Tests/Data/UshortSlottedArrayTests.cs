using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class UshortSlottedArrayTests
{
    private const ushort Key0 = 14;
    private const ushort Key1 = 28;
    private const ushort Key2 = 31;

    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        Span<byte> span = stackalloc byte[48];
        var map = new UShortSlottedArray(span);

        map.SetAssert(Key0, Data0);

        map.GetAssert(Key0, Data0);

        map.DeleteAssert(Key0);
        map.GetShouldFail(Key0);

        // should be ready to accept some data again
        map.SetAssert(Key0, Data1, "Should have memory after previous delete");
        map.GetAssert(Key0, Data1);
    }

    [Test]
    public void Defragment_when_no_more_space()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var map = new UShortSlottedArray(span);

        map.SetAssert(Key0, Data0);
        map.SetAssert(Key1, Data1);

        map.DeleteAssert(Key0);

        map.SetAssert(Key2, Data2, "Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        map.GetShouldFail(Key0);

        map.GetAssert(Key1, Data1);
        map.GetAssert(Key2, Data2);
    }

    [Test]
    public void Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[48];
        var map = new UShortSlottedArray(span);

        map.SetAssert(Key1, Data1);
        map.SetAssert(Key1, Data2);

        map.GetAssert(Key1, Data2);
    }

    [Test]
    public void Update_in_resize()
    {
        // Update the value, with the next one being bigger.
        Span<byte> span = stackalloc byte[40];
        var map = new UShortSlottedArray(span);

        map.SetAssert(Key0, Data0);
        map.SetAssert(Key0, Data2);

        map.GetAssert(Key0, Data2);
    }
}

file static class Extensions
{
    public static void SetAssert(this UShortSlottedArray map, ushort key, ReadOnlySpan<byte> data,
        string? because = null)
    {
        map.TrySet(key, data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void DeleteAssert(this UShortSlottedArray map, ushort key)
    {
        map.Delete(key).Should().BeTrue("Delete should succeed");
    }

    public static void GetAssert(this UShortSlottedArray map, ushort key, ReadOnlySpan<byte> expected)
    {
        map.TryGet(key, out var actual).Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }

    public static void GetShouldFail(this UShortSlottedArray map, ushort key)
    {
        map.TryGet(key, out var actual).Should().BeFalse("The key should not exist");
    }
}