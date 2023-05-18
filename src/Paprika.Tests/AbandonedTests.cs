using FluentAssertions;
using NUnit.Framework;
using Paprika.Store;

namespace Paprika.Tests;

public class AbandonedTests : BasePageTests
{
    private const int BatchId = 2;

    [Test]
    public void Simple()
    {
        var batch = NewBatch(BatchId);
        var abandoned = new AbandonedPage(batch.GetNewPage(out var addr, true));

        const int fromPage = 13;
        const int count = 1000;

        var pages = new HashSet<uint>();

        for (uint i = 0; i < count; i++)
        {
            var page = i + fromPage;
            pages.Add(page);

            abandoned.EnqueueAbandoned(batch, addr, DbAddress.Page(page));
        }

        for (uint i = 0; i < count; i++)
        {
            abandoned.TryDequeueFree(out var page).Should().BeTrue();
            pages.Remove(page).Should().BeTrue($"Page {page} should have been written first");
        }

        pages.Should().BeEmpty();
    }
}