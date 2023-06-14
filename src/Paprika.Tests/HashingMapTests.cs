using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests;

public class HashingMapTests
{
    private static NibblePath Path0 => NibblePath.FromKey(Values.Key0);
    private static NibblePath Path1 => NibblePath.FromKey(Values.Key1a);
    private static NibblePath Path2 => NibblePath.FromKey(Values.Key1b);

    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 37 };
    private static ReadOnlySpan<byte> Data2 => new byte[] { 41 };

    [Test]
    public void Set_Get_Should_use_hash_for_operations()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize];
        var map = new HashingMap(span);

        var key = Key.Account(Path0);

        var hash = HashingMap.GetHash(key);
        var data = Data0;

        hash.Should().NotBe(HashingMap.NoHash);

        map.SetAssert(hash, key, data);
        map.GetAssert(hash, key, data);

        var differentHash = hash + 1;
        map.TryGet(differentHash, key, out _).Should().BeFalse();
    }

    [Test]
    public void On_full_should_report_false()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize];
        var map = new HashingMap(span);

        var key0 = Key.Account(Path0);
        var hash0 = HashingMap.GetHash(key0);

        var key1 = Key.Account(Path0);
        var hash1 = HashingMap.GetHash(key1);

        var data = Data0;

        hash0.Should().NotBe(HashingMap.NoHash);

        map.SetAssert(hash0, key0, data);
        map.TrySet(hash1, key1, data).Should().BeFalse();

        map.GetAssert(hash0, key0, data);
    }

    [Test]
    public void Colliding_hashes()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize * 2];
        var map = new HashingMap(span);

        uint hash = 257;
        var key0 = Key.Account(Path0);
        var key1 = Key.Account(Path1);

        var data0 = Data0;
        var data1 = Data1;

        map.SetAssert(hash, key0, data0);
        map.SetAssert(hash, key1, data1);

        map.GetAssert(hash, key0, data0);
        map.GetAssert(hash, key1, data1);
    }

    [Test]
    public void Enumerate()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize * 3];
        var map = new HashingMap(span);

        var key0 = Key.Account(Path0);
        var hash0 = HashingMap.GetHash(key0);

        var key1 = Key.StorageCell(Path1, Keccak.EmptyTreeHash);
        var hash1 = HashingMap.GetHash(key1);

        var key2 = Key.Account(Path2);
        var hash2 = HashingMap.GetHash(key2);

        var data0 = Data0;
        var data1 = Data1;
        var data2 = Data2;

        map.SetAssert(hash0, key0, data0);
        map.SetAssert(hash1, key1, data1);
        map.SetAssert(hash2, key2, data2);

        var e = map.GetEnumerator();

        e.MoveNext().Should().BeTrue();
        e.Current.Hash.Should().Be(hash0);
        e.Current.RawData.SequenceEqual(data0);
        //e.Current.Key.Equals(key0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Hash.Should().Be(hash1);
        e.Current.RawData.SequenceEqual(data1);
        //e.Current.Key.Equals(key1).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Hash.Should().Be(hash2);
        e.Current.RawData.SequenceEqual(data2);
        //e.Current.Key.Equals(key1).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public void Clear()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize];
        var map = new HashingMap(span);

        var key = Key.Account(Path0);

        var hash = HashingMap.GetHash(key);
        var data = Data0;

        hash.Should().NotBe(HashingMap.NoHash);

        map.SetAssert(hash, key, data);

        map.Clear();

        map.TryGet(hash, key, out _).Should().BeFalse();
    }
}

static class TestExtensions
{
    public static void SetAssert(this HashingMap map, uint hash, in Key key, ReadOnlySpan<byte> data)
    {
        map.TrySet(hash, key, data).Should().BeTrue("TrySet should succeed");
    }

    public static void GetAssert(this HashingMap map, uint hash, in Key key, ReadOnlySpan<byte> expected)
    {
        map.TryGet(hash, key, out var actual).Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue($"Actual data {actual.ToHexString(false)} should equal expected {expected.ToHexString(false)}");
    }
}