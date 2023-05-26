using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;

namespace Paprika.Tests.Chain;

public class PagePoolTests
{
    [Test]
    public void Simple_reuse()
    {
        using var pool = new PagePool(1);

        // lease and return
        var initial = pool.Rent();
        pool.Return(initial);

        // dummy loop
        for (int i = 0; i < 100; i++)
        {
            var page = pool.Rent();
            page.Should().Be(initial);
            pool.Return(page);
        }

        pool.AllocatedPages.Should().Be(1);
    }
}