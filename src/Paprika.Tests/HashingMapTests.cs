using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests;

public class HashingMapTests
{
    private static NibblePath Path0 => NibblePath.FromKey(Values.Key0);
    private static NibblePath Path1 => NibblePath.FromKey(Values.Key1a);
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 37 };

    [Test]
    public void Set_Get_Should_use_hash_for_operations()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize];
        var map = new HashingMap(span);

        var key = Key.Account(Path0);

        var hash = HashingMap.GetHash(key);
        var data = Data0;

        hash.Should().NotBe(HashingMap.Null);

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

        hash0.Should().NotBe(HashingMap.Null);

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
    public void Clear()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize];
        var map = new HashingMap(span);

        var key = Key.Account(Path0);

        var hash = HashingMap.GetHash(key);
        var data = Data0;

        hash.Should().NotBe(HashingMap.Null);

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