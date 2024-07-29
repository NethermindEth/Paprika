using FluentAssertions;
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

    [Test]
    public void Rotating_updates()
    {
        Span<byte> span = stackalloc byte[128];
        var map = new UShortSlottedArray(span);

        var keys = new Queue<ushort>();
        ushort key = 0;

        while (map.TrySet(key, Data0))
        {
            keys.Enqueue(key);
            key++;
        }

        const int count = 1000;

        for (int i = 0; i < count; i++)
        {
            map.Delete(keys.Dequeue()).Should().BeTrue();
            map.Set(key, Data0);
            keys.Enqueue(key);
            key++;
        }
    }

    [Test]
    public void EnumerateAll()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var map = new UShortSlottedArray(span);

        map.SetAssert(Key0, Data0);
        map.SetAssert(Key1, Data1);

        var e = map.EnumerateAll().GetEnumerator();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Should().Be(Key0);
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Should().Be(Key1);
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
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