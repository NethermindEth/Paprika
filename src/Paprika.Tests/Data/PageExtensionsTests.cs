using System.Runtime.InteropServices;
using FluentAssertions;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Data;

public class PageExtensionsTests
{
    [Test]
    public void Or_with()
    {
        var page0 = Page.DevOnlyNativeAlloc();
        page0.Clear();

        var page1 = Page.DevOnlyNativeAlloc();
        const byte fill = 0xFF;
        page1.Span.Fill(fill); // fill whole page with FFFFF

        const int notFound = -1;
        page0.Span.IndexOfAnyExcept((byte)0).Should().Be(notFound);

        page0.OrWith(page1);

        page0.Span.IndexOfAnyExcept(fill).Should().Be(notFound);
    }
}