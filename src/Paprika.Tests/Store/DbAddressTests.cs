using FluentAssertions;
using NUnit.Framework;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class DbAddressTests
{
    [Test]
    public void Equality()
    {
        var a1 = DbAddress.Page(1);
        var a2 = DbAddress.Page(2);

        // ReSharper disable once EqualExpressionComparison
        Equals(a1, a1).Should().BeTrue();
        Equals(a1, a2).Should().BeFalse();
    }

    [Test]
    public void FileOffset_overflow()
    {
        long page = Page.PageCount - 1;
        var address = DbAddress.Page((uint)page);

        address.FileOffset.Should().Be(page * Page.PageSize);
    }
}