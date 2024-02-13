using System;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;

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
}