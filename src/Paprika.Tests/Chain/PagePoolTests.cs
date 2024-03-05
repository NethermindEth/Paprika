using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class PagePoolTests
{
    [Test]
    public void Simple_reuse()
    {
        using var pool = new BufferPool(1);

        // lease and return
        var initial = pool.Rent();
        pool.Return(initial);

        // dummy loop
        for (int i = 0; i < 1000; i++)
        {
            var page = pool.Rent();
            BinaryPrimitives.WriteInt64BigEndian(page.Span, i);

            page.Should().Be(initial);
            pool.Return(page);
        }

        pool.AllocatedMB.Should().Be(0, "No megabytes should be reported, it's one page only");
    }

    [Test]
    public void Rented_is_clear()
    {
        const int index = 5;
        using var pool = new BufferPool(1);

        // lease and return
        var initial = pool.Rent();
        initial.Span[index] = 13;

        pool.Return(initial);

        var page = pool.Rent();
        page.Span[index].Should().Be(0);

        pool.Return(initial);
    }

    [Test]
    public void Big_pool()
    {
        const int pageCount = 1024;
        using var pool = new BufferPool(pageCount, assertCountOnDispose: false);

        var set = new HashSet<UIntPtr>();

        for (int i = 0; i < pageCount; i++)
        {
            set.Add(pool.Rent().Raw);
        }

        set.Count.Should().Be(pageCount);
    }
}
