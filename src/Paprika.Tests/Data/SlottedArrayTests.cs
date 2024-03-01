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

    [Test]
    public void Set_Get_Delete_Get_AnotherSet([Values(0, 1)] byte meta0, [Values(0, 1)] byte meta1)
    {
        var key0 = Values.Key0.Span;

        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        map.SetAssertWithMeta(key0, Data0, meta0);

        map.GetAssert(key0, Data0, meta0);

        map.DeleteAssert(key0);
        map.GetShouldFail(key0);

        // should be ready to accept some data again
        map.SetAssertWithMeta(key0, Data1, meta1, "Should have memory after previous delete");
        map.GetAssert(key0, Data1, meta1);
    }

    [Test]
    public void Enumerate_all([Values(0, 1)] byte meta)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = Span<byte>.Empty;
        Span<byte> key1 = stackalloc byte[1] { 7 };
        Span<byte> key2 = stackalloc byte[2] { 7, 13 };

        map.SetAssertWithMeta(key0, Data0, meta);
        map.SetAssertWithMeta(key1, Data1, meta);
        map.SetAssertWithMeta(key2, Data2, meta);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.RawSpan.SequenceEqual(key0).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();
        e.Current.Metadata.Should().Be(meta);

        e.MoveNext().Should().BeTrue();
        e.Current.Key.RawSpan.SequenceEqual(key1).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();
        e.Current.Metadata.Should().Be(meta);

        e.MoveNext().Should().BeTrue();
        e.Current.Key.RawSpan.SequenceEqual(key2).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data2).Should().BeTrue();
        e.Current.Metadata.Should().Be(meta);

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public void Defragment_when_no_more_space([Values(0, 1)] byte meta)
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[88];
        var map = new SlottedArray(span);

        var key0 = Values.Key0.Span;
        var key1 = Values.Key1.Span;
        var key2 = Values.Key2.Span;

        map.SetAssertWithMeta(key0, Data0, meta);
        map.SetAssertWithMeta(key1, Data1, meta);

        map.DeleteAssert(key0);

        map.SetAssertWithMeta(key2, Data2, meta, "Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        map.GetShouldFail(key0);

        map.GetAssert(key1, Data1, meta);
        map.GetAssert(key2, Data2, meta);
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
    public void Report_has_space_properly()
    {
        const int dataSize = 1;
        const int keySize = 0;
        var key = NibblePath.Empty;
        Span<byte> value = stackalloc byte[dataSize] { 13 };
        Span<byte> valueToBig = stackalloc byte[dataSize + 1];

        Span<byte> span = stackalloc byte[SlottedArray.OneSlotArrayMinimalSize + dataSize + keySize];
        var map = new SlottedArray(span);

        map.SetAssert(key, value);

        map.HasSpaceToUpdateExisting(key, ReadOnlySpan<byte>.Empty).Should().BeTrue();
        map.HasSpaceToUpdateExisting(key, value).Should().BeTrue();
        map.HasSpaceToUpdateExisting(key, valueToBig).Should().BeFalse();
    }

    [Test]
    public void Update_in_resize([Values(0, 1)] byte meta0, [Values(0, 1)] byte meta1)
    {
        // Update the value, with the next one being bigger.
        Span<byte> span = stackalloc byte[56];
        var map = new SlottedArray(span);

        var key0 = Values.Key0.Span;

        map.SetAssertWithMeta(key0, Data0, meta0);
        map.SetAssertWithMeta(key0, Data2, meta1);

        map.GetAssert(key0, Data2, meta1);
    }

    [Test]
    public void Hashing()
    {
        var hashes = new Dictionary<ushort, string>();

        // empty
        Unique(ReadOnlySpan<byte>.Empty);

        // single byte
        Unique(stackalloc byte[] { 1 });
        Unique(stackalloc byte[] { 2 });
        Unique(stackalloc byte[] { 16 });
        Unique(stackalloc byte[] { 17 });


        // two bytes
        Unique(stackalloc byte[] { 0xA, 0xC });
        Unique(stackalloc byte[] { 0xA, 0xB });
        Unique(stackalloc byte[] { 0xB, 0xC });

        // three bytes
        Unique(stackalloc byte[] { 0xA, 0xD, 0xC });
        Unique(stackalloc byte[] { 0xAA, 0xEE, 0xCC });
        Unique(stackalloc byte[] { 0xAA, 0xFF, 0xCC });

        // three bytes
        Unique(stackalloc byte[] { 0xAA, 0xDD, 0xCC, 0x11 });
        Unique(stackalloc byte[] { 0xAA, 0xEE, 0xCC, 0x11 });
        Unique(stackalloc byte[] { 0xAA, 0xFF, 0xCC, 0x11 });

        return;

        void Unique(in ReadOnlySpan<byte> key)
        {
            var hash = SlottedArray.GetHash(NibblePath.FromKey(key));
            var hex = key.ToHexString(true);

            if (hashes.TryAdd(hash, hex) == false)
            {
                Assert.Fail($"The hash for {hex} is the same as for {hashes[hash]}");
            }
        }
    }

    [Test]
    public void Small_keys_compression([Values(0, 1)] byte meta)
    {
        Span<byte> span = stackalloc byte[128];
        var map = new SlottedArray(span);

        Span<byte> raw = stackalloc byte[1];
        Span<byte> value = stackalloc byte[2];

        const int maxPathShortened = 1;
        const int count = 16; // 16 - 1

        for (byte i = 0; i < count; i++)
        {
            var key = BuildKey(raw, i);
            value[0] = i;
            value[1] = i;

            map.SetAssertWithMeta(key, value, meta, $"{i}th was not set");
        }

        for (byte i = 0; i < count; i++)
        {
            var key = BuildKey(raw, i);
            value[0] = i;
            value[1] = i;

            map.GetAssert(key, value, meta);
        }

        return;

        static NibblePath BuildKey(Span<byte> destination, byte i)
        {
            destination[0] = (byte)(i << NibblePath.NibbleShift);
            return NibblePath.FromKey(destination).SliceTo(maxPathShortened);
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

    public static void SetAssertWithMeta(this SlottedArray map, in NibblePath key, ReadOnlySpan<byte> data, byte meta,
        string? because = null)
    {
        map.TrySet(key, data, meta).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void SetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data,
        string? because = null)
    {
        map.TrySet(NibblePath.FromKey(key), data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void SetAssertWithMeta(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data,
        byte metadata = 0,
        string? because = null)
    {
        map.TrySet(NibblePath.FromKey(key), data, metadata).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void DeleteAssert(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.Delete(NibblePath.FromKey(key)).Should().BeTrue("Delete should succeed");
    }

    public static void GetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> expected,
        byte metadata = 0)
    {
        map.TryGetWithMetadata(NibblePath.FromKey(key), out var actual, out var actualMeta).Should().BeTrue();
        actualMeta.Should().Be(metadata);
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }

    public static void GetAssert(this SlottedArray map, in NibblePath key, ReadOnlySpan<byte> expected,
        byte metadata = 0)
    {
        map.TryGetWithMetadata(key, out var actual, out var actualMeta).Should().BeTrue();
        actualMeta.Should().Be(metadata);
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }

    public static void GetShouldFail(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.TryGet(NibblePath.FromKey(key), out var actual).Should().BeFalse("The key should not exist");
    }
}