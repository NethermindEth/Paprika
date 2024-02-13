using System;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Tests.Chain;

[TestFixture(true, true)]
[TestFixture(true, false)]
[TestFixture(false, true)]
[TestFixture(false, false)]
public class PooledSpanDictionaryTests(bool preserveOldValues, bool concurrentReaders)
{
    [Test]
    public void Destruction()
    {
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool, preserveOldValues, concurrentReaders);

        Span<byte> key = stackalloc byte[] { 13, 17 };
        Span<byte> value = stackalloc byte[] { 211 };
        const byte metadata = 1;
        const ulong hash = 859;

        dict.Set(key, hash, value, metadata);
        dict.TryGet(key, hash, out var actual).Should().BeTrue();
        actual.SequenceEqual(value).Should().BeTrue();

        dict.Destroy(key, hash);
        dict.TryGet(key, hash, out var destroyed).Should().BeTrue();
        destroyed.IsEmpty.Should().BeTrue();
    }

    [Test]
    public void On_page_boundary()
    {
        // Set two kvp, key0 + data0 + key1 fill first page, data1, will be emptys
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool, preserveOldValues, concurrentReaders);

        Span<byte> key0 = stackalloc byte[] { 13, 17 };
        const ulong hash0 = 859;

        Span<byte> key1 = stackalloc byte[] { 23, 19 };
        const ulong hash1 = 534;

        var onePage = new byte[BufferPool.BufferSize - key0.Length - key1.Length];

        const byte metadata = 1;

        dict.Set(key0, hash0, onePage, metadata);
        dict.Set(key1, hash1, ReadOnlySpan<byte>.Empty, metadata);

        dict.TryGet(key0, hash0, out var value0).Should().BeTrue();
        value0.SequenceEqual(onePage);

        dict.TryGet(key1, hash1, out var value1).Should().BeTrue();
        value1.SequenceEqual(ReadOnlySpan<byte>.Empty);
    }
}