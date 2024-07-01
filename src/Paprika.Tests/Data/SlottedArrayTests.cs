using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Data;

public class SlottedArrayTests
{
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    private static ReadOnlySpan<byte> Data3 => new byte[] { 31, 41 };
    private static ReadOnlySpan<byte> Data4 => new byte[] { 23, 24, 25 };
    private static ReadOnlySpan<byte> Data5 => new byte[] { 23, 24, 64 };

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
    public Task Enumerate_long_key([Values(0, 1)] int oddStart, [Values(0, 1)] int lengthCutOff)
    {
        Span<byte> span = stackalloc byte[128];
        var map = new SlottedArray(span);

        var key = NibblePath.FromKey(Keccak.EmptyTreeHash).SliceFrom(oddStart)
            .SliceTo(NibblePath.KeccakNibbleCount - oddStart - lengthCutOff);

        map.SetAssert(key, Data0);
        map.GetAssert(key, Data0);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

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

    private static ReadOnlySpan<byte> Data(byte key) => new[] { key };

    [Test]
    public void Move_to_1()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);

        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(NibblePath.Empty, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new(copy0));

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(NibblePath.Empty, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy should have all but empty
        copy0.Count.Should().Be(5);
        copy0.GetShouldFail(NibblePath.Empty);
        copy0.GetAssert(key1, Data(1));
        copy0.GetAssert(key2, Data(2));
        copy0.GetAssert(key3, Data(3));
        copy0.GetAssert(key4, Data(4));
        copy0.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_respects_tombstones()
    {
        const int size = 256;

        var original = new SlottedArray(stackalloc byte[size]);
        var copy0 = new SlottedArray(stackalloc byte[size]);

        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        var tombstone = ReadOnlySpan<byte>.Empty;

        original.SetAssert(key1, tombstone);
        original.SetAssert(key2, tombstone);
        original.SetAssert(key3, tombstone);
        original.SetAssert(key4, tombstone);
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new(copy0), true);

        // original should have only empty
        original.Count.Should().Be(0);
        original.CapacityLeft.Should().Be(size - SlottedArray.HeaderSize);
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy should have all but empty
        copy0.Count.Should().Be(1);
        copy0.GetShouldFail(key1);
        copy0.GetShouldFail(key2);
        copy0.GetShouldFail(key3);
        copy0.GetShouldFail(key4);
        copy0.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_2()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);
        var copy1 = new SlottedArray(stackalloc byte[256]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(key0, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new(copy0, copy1));

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(key0, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy0 should have key2 and key4 and nothing else
        copy0.Count.Should().Be(2);
        copy0.GetShouldFail(key0);
        copy0.GetShouldFail(key1);
        copy0.GetAssert(key2, Data(2));
        copy0.GetShouldFail(key3);
        copy0.GetAssert(key4, Data(4));
        copy0.GetShouldFail(key5);

        // copy1 should have key1 and key3 and key5 and nothing else
        copy1.Count.Should().Be(3);
        copy1.GetShouldFail(key0);
        copy1.GetAssert(key1, Data(1));
        copy1.GetShouldFail(key2);
        copy1.GetAssert(key3, Data(3));
        copy1.GetShouldFail(key4);
        copy1.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_4()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);
        var copy1 = new SlottedArray(stackalloc byte[256]);
        var copy2 = new SlottedArray(stackalloc byte[256]);
        var copy3 = new SlottedArray(stackalloc byte[256]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(key0, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new(copy0, copy1, copy2, copy3));

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(key0, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy0 should have key4
        copy0.Count.Should().Be(1);
        copy0.GetShouldFail(key0);
        copy0.GetShouldFail(key1);
        copy0.GetShouldFail(key2);
        copy0.GetShouldFail(key3);
        copy0.GetAssert(key4, Data(4));
        copy0.GetShouldFail(key5);

        // copy1 should have key1 and key5 and nothing else
        copy1.Count.Should().Be(2);
        copy1.GetShouldFail(key0);
        copy1.GetAssert(key1, Data(1));
        copy1.GetShouldFail(key2);
        copy1.GetShouldFail(key3);
        copy1.GetShouldFail(key4);
        copy1.GetAssert(key5, Data(5));

        // copy1 should have key2 and nothing else
        copy2.Count.Should().Be(1);
        copy2.GetShouldFail(key0);
        copy2.GetShouldFail(key1);
        copy2.GetAssert(key2, Data(2));
        copy2.GetShouldFail(key3);
        copy2.GetShouldFail(key4);
        copy2.GetShouldFail(key5);

        // copy1 should have key2 and nothing else
        copy3.Count.Should().Be(1);
        copy3.GetShouldFail(key0);
        copy3.GetShouldFail(key1);
        copy3.GetShouldFail(key2);
        copy3.GetAssert(key3, Data(3));
        copy3.GetShouldFail(key4);
        copy3.GetShouldFail(key5);
    }

    [Test]
    public void Move_to_8()
    {
        var original = new SlottedArray(stackalloc byte[512]);
        var copy0 = new SlottedArray(stackalloc byte[64]);
        var copy1 = new SlottedArray(stackalloc byte[64]);
        var copy2 = new SlottedArray(stackalloc byte[64]);
        var copy3 = new SlottedArray(stackalloc byte[64]);
        var copy4 = new SlottedArray(stackalloc byte[64]);
        var copy5 = new SlottedArray(stackalloc byte[64]);
        var copy6 = new SlottedArray(stackalloc byte[64]);
        var copy7 = new SlottedArray(stackalloc byte[64]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");
        var key6 = NibblePath.Parse("6789AB");
        var key7 = NibblePath.Parse("789ABCD");
        var key8 = NibblePath.Parse("89ABCDEF");

        original.SetAssert(key0, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));
        original.SetAssert(key6, Data(6));
        original.SetAssert(key7, Data(7));
        original.SetAssert(key8, Data(8));

        original.MoveNonEmptyKeysTo(new(copy0, copy1, copy2, copy3, copy4, copy5, copy6, copy7));

        // original should have only empty
        HasOnly(original, 0);

        HasOnly(copy0, 8);
        HasOnly(copy1, 1);
        HasOnly(copy2, 2);
        HasOnly(copy3, 3);
        HasOnly(copy4, 4);
        HasOnly(copy5, 5);
        HasOnly(copy6, 6);
        HasOnly(copy7, 7);

        return;

        static void HasOnly(in SlottedArray map, int key)
        {
            map.Count.Should().Be(1);

            for (byte i = 0; i < 8; i++)
            {
                var k = i switch
                {
                    0 => NibblePath.Empty,
                    1 => NibblePath.Parse("1"),
                    2 => NibblePath.Parse("23"),
                    3 => NibblePath.Parse("345"),
                    4 => NibblePath.Parse("4567"),
                    5 => NibblePath.Parse("56789"),
                    6 => NibblePath.Parse("6789AB"),
                    7 => NibblePath.Parse("789ABCD"),
                    _ => NibblePath.Parse("89ABCDEF")
                };

                if (i == key)
                {
                    map.GetAssert(k, Data(i));
                }
                else
                {
                    map.GetShouldFail(k);
                }
            }
        }
    }

    [Test]
    public void Hashing()
    {
        var hashes = new Dictionary<int, string>();

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
        map.TryGet(NibblePath.FromKey(key), out _).Should().BeFalse("The key should not exist");
    }

    public static void GetShouldFail(this SlottedArray map, in NibblePath key)
    {
        map.TryGet(key, out _).Should().BeFalse("The key should not exist");
    }
}