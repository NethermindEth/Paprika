using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class BloomFilterTests
{
    [Test]
    public void Set_is_set()
    {
        const int size = 1000;
        var page = Page.DevOnlyNativeAlloc();
        var bloom = new BloomFilter(page);

        var random = new Random(13);
        for (var i = 0; i < size; i++)
        {
            bloom.Set(random.Next(i));
        }

        // assert sets
        random = new Random(13);
        for (var i = 0; i < size; i++)
        {
            bloom.IsSet(random.Next(i)).Should().BeTrue();
        }
    }
}
