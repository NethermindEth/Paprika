using System.Buffers.Binary;
using FluentAssertions;
using JetBrains.dotMemoryUnit;
using NUnit.Framework;
using Paprika.Chain;

namespace Paprika.Tests.Chain;

public class PooledSpanDictionaryTests
{
    [TestCase(true)]
    [TestCase(false)]
    public void Destruction(bool preserveOldValues)
    {
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool, preserveOldValues);

        Span<byte> key = stackalloc byte[] { 13, 17 };
        Span<byte> value = stackalloc byte[] { 211 };
        const byte metadata = 1;
        const ulong hash = 859;

        dict.Set(key, hash, value, metadata);
        dict.TryGet(key, hash, out var actual).Should().BeTrue();
        actual.SequenceEqual(value).Should().BeTrue();

        dict.Destroy(key, hash);
        dict.TryGet(key, hash, out var destroyed).Should().BeFalse();
        destroyed.IsEmpty.Should().BeTrue();
    }

    [Test]
    public void Update_should_invalidate_previous_even_when_preserving_old_values()
    {
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool, true);

        Span<byte> key = stackalloc byte[] { 13, 17 };
        
        Span<byte> value0 = stackalloc byte[] { 211 };
        const byte meta0 = 1;
        
        Span<byte> value1 = stackalloc byte[] { 23 };
        const byte meta1 = 0;

        const ulong hash = 859;

        // Set get value 0
        dict.Set(key, hash, value0, meta0);
        dict.TryGet(key, hash, out var actual0).Should().BeTrue();
        actual0.SequenceEqual(value0).Should().BeTrue();
        
        // Set get value 1
        dict.Set(key, hash, value1, meta1);
        dict.TryGet(key, hash, out var actual1).Should().BeTrue();
        actual1.SequenceEqual(value1).Should().BeTrue();
        
        // value 0 should still be equal as preserves old values
        actual0.SequenceEqual(value0).Should().BeTrue();
        
        using var e = dict.GetEnumerator();
        
        e.MoveNext().Should().BeTrue();
        e.Current.Metadata.Should().Be(meta1);
        e.Current.Hash.Should().Be((uint)hash);
        
        e.MoveNext().Should().BeFalse();
    }

    [TestCase(127, 1)]
    [TestCase(256, 0)]
    public void Set_get_enumerate_destroy(int valueLength, byte metadata)
    {
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool);

        Span<byte> key = stackalloc byte[] { 13, 17 };
        Span<byte> value = new byte[valueLength];
        value.Fill(0x13);

        const ulong hash = 859;

        dict.Set(key, hash, value, metadata);

        dict.TryGet(key, hash, out var result).Should().BeTrue();
        result.SequenceEqual(value).Should().BeTrue();

        using var e = dict.GetEnumerator();

        e.MoveNext().Should().BeTrue();
        e.Current.Metadata.Should().Be(metadata);
        e.Current.Hash.Should().Be((uint)hash);
        e.Current.Key.SequenceEqual(key).Should().BeTrue();
        e.Current.Value.SequenceEqual(value).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        dict.Destroy(key, hash);
        dict.TryGet(key, hash, out _).Should().BeFalse();

        using var e2 = dict.GetEnumerator();
        e2.MoveNext().Should().BeFalse();
    }
    
    [Test]
    public void Update_to_larger_value()
    {
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool);

        Span<byte> key = stackalloc byte[] { 13, 17 };
        
        Span<byte> value0 = new byte[1];
        value0.Fill(0x13);
        const byte meta0 = 0;
        
        Span<byte> value1 = new byte[13];
        value1.Fill(0x17);
        const byte meta1 = 1;

        const ulong hash = 859;

        // Set & Assert value0
        dict.Set(key, hash, value0, meta0);
        dict.TryGet(key, hash, out var result).Should().BeTrue();
        result.SequenceEqual(value0).Should().BeTrue();
        
        // Set & Assert value1
        dict.Set(key, hash, value1, meta1);
        dict.TryGet(key, hash, out result).Should().BeTrue();
        result.SequenceEqual(value1).Should().BeTrue();

        // Enumerate
        using var e = dict.GetEnumerator();

        e.MoveNext().Should().BeTrue();
        e.Current.Metadata.Should().Be(meta1);
        e.Current.Hash.Should().Be((uint)hash);
        e.Current.Key.SequenceEqual(key).Should().BeTrue();
        e.Current.Value.SequenceEqual(value1).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }
    
    [Test]
    public void On_page_boundary()
    {
        // Set two kvp, key0 + data0 + key1 fill first page, data1, will be empty
        using var pool = new BufferPool(4);
        using var dict = new PooledSpanDictionary(pool, false);

        Span<byte> key0 = stackalloc byte[] { 13, 17 };
        const ulong hash0 = 859;

        Span<byte> key1 = stackalloc byte[] { 23, 19 };
        const ulong hash1 = 534;

        var onePage =
            new byte[BufferPool.BufferSize - key0.Length - key1.Length - PooledSpanDictionary.ItemOverhead * 2];

        const byte metadata = 1;

        dict.Set(key0, hash0, onePage, metadata);
        dict.Set(key1, hash1, ReadOnlySpan<byte>.Empty, metadata);

        dict.TryGet(key0, hash0, out var value0).Should().BeTrue();
        value0.SequenceEqual(onePage);

        dict.TryGet(key1, hash1, out var value1).Should().BeTrue();
        value1.SequenceEqual(ReadOnlySpan<byte>.Empty);
    }

    private const int Mb = 1024 * 1024;

    [Test]
    [Category(Categories.LongRunning)]
    [Category(Categories.Memory)]
    [AssertTraffic(AllocatedSizeInBytes = 1 * Mb)]
    public void Large_spin()
    {
        const uint size = 1_000;
        
        // Set two kvp, key0 + data0 + key1 fill first page, data1, will be empty
        using var pool = new BufferPool(128);

        byte[] key = new byte[13];

        using var dict = new PooledSpanDictionary(pool, false);

        for (uint i = 0; i < size; i++)
        {
            Set(i, key);
            var hash = i;
            dict.Set(key, hash, key, GetMetadata(key));
            dict.TryGet(key, hash, out _);
        }

        var count = 0;
        foreach (var kvp in dict)
        {
            kvp.Key.SequenceEqual(kvp.Value).Should().BeTrue();
            kvp.Metadata.Should().Be(GetMetadata(kvp.Key));
            count++;
        }

        count.Should().Be((int)size);

        return;

        // dotMemory.Check();
        static void Set(uint i, byte[] key) => BinaryPrimitives.WriteUInt32LittleEndian(key, i);
        static byte GetMetadata(ReadOnlySpan<byte> key) => (byte)(key[0] & 1);
    }
}