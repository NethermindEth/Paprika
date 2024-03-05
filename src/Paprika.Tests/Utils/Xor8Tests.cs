using System.Runtime.InteropServices;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Utils;

namespace Paprika.Tests.Utils;

public class Xor8Tests
{
    [TestCase(10000)]
    public void Empty(int count)
    {
        var filter = new Xor8(Array.Empty<ulong>());

        const double falsePositiveRatio = 0.1;
        var found = 0;

        for (var i = 0; i < count; i++)
        {
            var key = unchecked((ulong)TestContext.CurrentContext.Random.NextInt64());
            if (filter.MayContain(key))
            {
                found++;
            }
        }

        ((double)found / count).Should().BeLessThan(falsePositiveRatio);
    }

    [TestCase(10_000)]
    public void Test(int count)
    {
        var keys = new ulong[count];
        TestContext.CurrentContext.Random.NextBytes(MemoryMarshal.Cast<ulong, byte>(keys));

        var filter = new Xor8(keys);

        foreach (var key in keys)
        {
            filter.MayContain(key).Should().BeTrue();
        }
    }
}
