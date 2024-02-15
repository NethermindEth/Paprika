using System;
using System.Buffers.Binary;
using FluentAssertions;
using JetBrains.dotMemoryUnit;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class PooledSpanDictionaryTests
{
    [TestCase(true, true)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public void Destruction(bool preserveOldValues, bool concurrentReaders)
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
        // Set two kvp, key0 + data0 + key1 fill first page, data1, will be empty
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool, false, false);

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

    public const int Mb = 1024 * 1024;

    [Test]
    [Category(Categories.LongRunning)]
    [AssertTraffic(AllocatedSizeInBytes = 1 * Mb)]
    public void Large_spin()
    {
        // Set two kvp, key0 + data0 + key1 fill first page, data1, will be emptys
        using var pool = new BufferPool(128);

        byte[] key = new byte[64];

        for (var i = 0; i < 100; i++)
        {
            using var dict = new PooledSpanDictionary(pool, false, true);

            for (uint keys = 0; keys < 10_000; keys++)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(key, keys);
                var hash = keys;
                dict.Set(key, hash, key, 0);
                dict.TryGet(key, hash, out _);
            }
        }

        dotMemory.Check();
    }
}