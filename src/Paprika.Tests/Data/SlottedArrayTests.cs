using FluentAssertions;
using NUnit.Framework;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class SlottedArrayTests
{
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        var key0 = Values.Key0.Span;

        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        map.SetAssert(key0, Data0);

        map.GetAssert(key0, Data0);

        map.DeleteAssert(key0);
        map.GetShouldFail(key0);

        // should be ready to accept some data again
        map.SetAssert(key0, Data1, "Should have memory after previous delete");
        map.GetAssert(key0, Data1);
    }

    [Test]
    public void Enumerate_all([Values(0,1)] int odd)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = Span<byte>.Empty;
        var key1 = NibblePath.FromKey(stackalloc byte[1] { 7 }).SliceFrom(odd);
        var key2 = NibblePath.FromKey(stackalloc byte[2] { 7, 13 }).SliceFrom(odd);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.RawSpan.SequenceEqual(key0).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key1).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key2).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data2).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public void Defragment_when_no_more_space()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[88];
        var map = new SlottedArray(span);

        var key0 = Values.Key0.Span;
        var key1 = Values.Key1.Span;
        var key2 = Values.Key2.Span;

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);

        map.DeleteAssert(key0);

        map.SetAssert(key2, Data2, "Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        map.GetShouldFail(key0);

        map.GetAssert(key1, Data1);
        map.GetAssert(key2, Data2);
    }

    [Test]
    public void Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        var key1 = Values.Key1.Span;

        map.SetAssert(key1, Data1);
        map.SetAssert(key1, Data2);

        map.GetAssert(key1, Data2);
    }

    [Test]
    public void Update_in_resize()
    {
        // Update the value, with the next one being bigger.
        Span<byte> span = stackalloc byte[56];
        var map = new SlottedArray(span);

        var key0 = Values.Key0.Span;

        map.SetAssert(key0, Data0);
        map.SetAssert(key0, Data2);

        map.GetAssert(key0, Data2);
    }

    [Test]
    public void Small_keys_compression()
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        Span<byte> key = stackalloc byte[1];
        Span<byte> value = stackalloc byte[2];

        const int count = 34;

        for (byte i = 0; i < count; i++)
        {
            key[0] = i;
            value[0] = i;
            value[1] = i;

            map.SetAssert(key, value, $"{i}th was not set");
        }

        for (byte i = 0; i < count; i++)
        {
            key[0] = i;
            value[0] = i;
            value[1] = i;

            map.GetAssert(key, value);
        }
    }
    //
    // [Test]
    // public void Hashing()
    // {
    //     var hashes = new Dictionary<ushort, string>();
    //
    //     // empty
    //     Unique(ReadOnlySpan<byte>.Empty);
    //
    //     // single byte
    //     Unique(stackalloc byte[] { 1 });
    //     Unique(stackalloc byte[] { 2 });
    //     Unique(stackalloc byte[] { 16 });
    //     Unique(stackalloc byte[] { 17 });
    //
    //
    //     // two bytes
    //     Unique(stackalloc byte[] { 0xA, 0xC });
    //     Unique(stackalloc byte[] { 0xA, 0xB });
    //     Unique(stackalloc byte[] { 0xB, 0xC });
    //
    //     // three bytes
    //     Unique(stackalloc byte[] { 0xA, 0xD, 0xC });
    //     Unique(stackalloc byte[] { 0xAA, 0xEE, 0xCC });
    //     Unique(stackalloc byte[] { 0xAA, 0xFF, 0xCC });
    //
    //     // three bytes
    //     Unique(stackalloc byte[] { 0xAA, 0xDD, 0xCC, 0x11 });
    //     Unique(stackalloc byte[] { 0xAA, 0xEE, 0xCC, 0x11 });
    //     Unique(stackalloc byte[] { 0xAA, 0xFF, 0xCC, 0x11 });
    //
    //     return;
    //
    //     void Unique(in ReadOnlySpan<byte> key)
    //     {
    //         var hash = SlottedArray.PrepareKey(NibblePath.FromKey(key));
    //         var hex = key.ToHexString(true);
    //
    //         if (hashes.TryAdd(hash, hex) == false)
    //         {
    //             Assert.Fail($"The hash for {hex} is the same as for {hashes[hash]}");
    //         }
    //     }
    // }
}

file static class FixedMapTestExtensions
{
    public static void SetAssert(this SlottedArray map, in NibblePath key, ReadOnlySpan<byte> data,
        string? because = null)
    {
        map.TrySet(key, data).Should().BeTrue(because ?? "TrySet should succeed");
    }
    
    public static void SetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data,
        string? because = null)
    {
        map.TrySet(NibblePath.FromKey(key), data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void DeleteAssert(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.Delete(NibblePath.FromKey(key)).Should().BeTrue("Delete should succeed");
    }

    public static void GetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> expected)
    {
        map.TryGet(NibblePath.FromKey(key), out var actual).Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }

    public static void GetShouldFail(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.TryGet(NibblePath.FromKey(key), out var actual).Should().BeFalse("The key should not exist");
    }
}