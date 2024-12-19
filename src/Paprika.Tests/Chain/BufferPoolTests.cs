using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class BufferPoolTests
{
    [Test]
    public void Simple_reuse()
    {
        using var meter = new Meter(nameof(Simple_reuse));
        using var pool = new BufferPool(1, BufferPool.PageTracking.AssertCount, meter);

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
    public void Stack_trace_tracking()
    {
        var pool = new BufferPool(1, BufferPool.PageTracking.StackTrace);

        // Rent and not return
        pool.Rent();

        var exception = Assert.Throws<Exception>(() => pool.Dispose());

        exception.StackTrace
            .Should()
            .Contain(nameof(BufferPoolTests)).And
            .Contain(nameof(Stack_trace_tracking));
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
        using var pool = new BufferPool(pageCount, BufferPool.PageTracking.None);

        var set = new HashSet<UIntPtr>();

        for (int i = 0; i < pageCount; i++)
        {
            set.Add(pool.Rent().Raw);
        }

        set.Count.Should().Be(pageCount);
    }
}
