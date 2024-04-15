using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class SlottedArrayTests
{
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    private static ReadOnlySpan<byte> Data3 => new byte[] { 31, 41 };
    private static ReadOnlySpan<byte> Data4 => new byte[] { 23, 24, 25 };

    [Test]
    public Task Set_Get_Delete_Get_AnotherSet()
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

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Enumerate_all([Values(0, 1)] int odd)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.FromKey(stackalloc byte[1] { 7 }).SliceFrom(odd);
        var key2 = NibblePath.FromKey(stackalloc byte[2] { 7, 13 }).SliceFrom(odd);
        var key3 = NibblePath.FromKey(stackalloc byte[3] { 7, 13, 31 }).SliceFrom(odd);
        var key4 = NibblePath.FromKey(stackalloc byte[4] { 7, 13, 31, 41 }).SliceFrom(odd);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);
        map.SetAssert(key3, Data3);
        map.SetAssert(key4, Data4);

        map.GetAssert(key0, Data0);
        map.GetAssert(key1, Data1);
        map.GetAssert(key2, Data2);
        map.GetAssert(key4, Data4);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key0).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key1).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key2).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data2).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key3).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data3).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key4).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data4).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Set_Get_Empty()
    {
        var key0 = Values.Key0.Span;

        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        var data = ReadOnlySpan<byte>.Empty;

        map.SetAssert(key0, data);
        map.GetAssert(key0, data);

        map.DeleteAssert(key0);
        map.GetShouldFail(key0);

        // should be ready to accept some data again
        map.SetAssert(key0, data, "Should have memory after previous delete");
        map.GetAssert(key0, data);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(NibblePath.FromKey(key0));
        e.Current.RawData.SequenceEqual(data).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Defragment_when_no_more_space()
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

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        var key1 = Values.Key1.Span;

        map.SetAssert(key1, Data1);
        map.SetAssert(key1, Data2);

        map.GetAssert(key1, Data2);

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Update_in_resize()
    {
        // Update the value, with the next one being bigger.
        Span<byte> span = stackalloc byte[56];
        var map = new SlottedArray(span);

        var key0 = Values.Key0.Span;

        map.SetAssert(key0, Data0);
        map.SetAssert(key0, Data2);

        map.GetAssert(key0, Data2);

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Small_keys_compression()
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

        // verify
        return Verify(span.ToArray());
    }

    [TestCase(0)]
    [TestCase(1)]
    public void Gathering_stats_count(int odd)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var data = ReadOnlySpan<byte>.Empty;

        // set empty
        map.SetAssert(NibblePath.Empty, data);

        // set 16 
        for (byte i = 0; i < 16; i++)
        {
            map.SetAssert(NibblePath.Single(i, odd), data);
        }

        // make key 0xAAAA
        const int additional = 10;
        Span<byte> key = stackalloc byte[2];
        key[0] = additional << NibblePath.NibbleShift | additional;
        key[1] = additional << NibblePath.NibbleShift | additional;

        var k = NibblePath.FromKey(key, odd, 2);
        map.SetAssert(k, data);

        Span<ushort> buckets = stackalloc ushort[16];
        map.GatherCountStatistics(buckets);

        for (var i = 0; i < buckets.Length; i++)
        {
            var bucket = buckets[i];
            bucket.Should().Be((ushort)(i == additional ? 2 : 1));
        }
    }

    [Test]
    public void Hashing()
    {
        var hashes = new Dictionary<ushort, string>();

        // empty
        Unique("");

        // single nibble
        Unique("A");
        Unique("B");
        Unique("C");
        Unique("7");

        // two nibbles
        Unique("AC");
        Unique("AB");
        Unique("BC");

        // three nibbles
        Unique("ADC");
        Unique("AEB");
        Unique("BEC");

        // four nibbles
        Unique("ADC1");
        Unique("AEB1");
        Unique("BEC1");

        // 5 nibbles, with last changed
        Unique("AD0C2");
        Unique("AE0B2");
        Unique("BE0C2");

        // 6 nibbles, with last changed
        Unique("AD00C3");
        Unique("AE00B3");
        Unique("BE00C3");

        return;

        void Unique(string key)
        {
            var path = NibblePath.Parse(key);
            var hash = SlottedArray.HashForTests(path);

            if (hashes.TryAdd(hash, key) == false)
            {
                Assert.Fail($"The hash for {key} is the same as for {hashes[hash]}");
            }
        }
    }
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

    public static void GetAssert(this SlottedArray map, in NibblePath key, ReadOnlySpan<byte> expected)
    {
        map.TryGet(key, out var actual).Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }


    public static void GetShouldFail(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.TryGet(NibblePath.FromKey(key), out var actual).Should().BeFalse("The key should not exist");
    }
}
