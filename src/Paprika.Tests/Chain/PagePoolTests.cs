﻿using System.Buffers.Binary;
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
            BinaryPrimitives.WriteInt64BigEndian(page.Span, i);

            page.Should().Be(initial);
            pool.Return(page);
        }

        pool.AllocatedPages.Should().Be(1);
    }

    [Test]
    public void Rented_is_clear()
    {
        const int index = 5;
        using var pool = new PagePool(1);

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
        using var pool = new PagePool(pageCount, assertCountOnDispose: false);

        var set = new HashSet<UIntPtr>();

        for (int i = 0; i < pageCount; i++)
        {
            set.Add(pool.Rent().Raw);
        }

        set.Count.Should().Be(pageCount);
    }
}