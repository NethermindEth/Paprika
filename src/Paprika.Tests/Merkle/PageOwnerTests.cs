using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Merkle;

public class PageOwnerTests
{
    private const UIntPtr Empty = default;

    [Test]
    public void Rent_then_return()
    {
        using var pool = new BufferPool(1, BufferPool.PageTracking.AssertCount);

        var stack = Empty;
        using (var o1 = PageOwner.Rent(pool, ref stack))
        {
            AssertOwner(o1);

            using (var o2 = PageOwner.Rent(pool, ref stack))
            {
                AssertOwner(o2);

                using (var o3 = PageOwner.Rent(pool, ref stack))
                {
                    AssertOwner(o3);
                }
            }
        }

        PageOwner.ReturnStack(pool, ref stack);
        stack.Should().Be(Empty);

        return;

        static void AssertOwner(in PageOwner owner)
        {
            owner.Span.Length.Should().Be(Page.PageSize);
            owner.Span.Fill(1);
        }
    }
}