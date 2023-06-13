using FluentAssertions;
using NUnit.Framework;
using Paprika.Data;

namespace Paprika.Tests;

public class HashingMapTests
{
    private static NibblePath Key0 => NibblePath.FromKey(Values.Key0);
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };

    [Test]
    public void Set_Get()
    {
        Span<byte> span = stackalloc byte[HashingMap.MinSize];
        var map = new HashingMap(span);

        var key = Key.Account(Key0);

        var hash = HashingMap.GetHash(key);
        var data = Data0;

        hash.Should().NotBe(HashingMap.NoCache);

        map.SetAssert(hash, key, data);
        map.GetAssert(hash, key, data);

        map.TryGet(hash + 1, key, out _).Should().BeFalse();
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
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }
}