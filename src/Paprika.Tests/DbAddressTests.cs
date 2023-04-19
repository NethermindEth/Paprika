using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class DbAddressTests
{
    [Test]
    public void Should_chain_in_page_properly()
    {
        DbAddress.Null.SamePageJumpCount.Should().Be(0);

        var jump1 = DbAddress.JumpToFrame(1, DbAddress.Null);
        jump1.SamePageJumpCount.Should().Be(1);

        var jump2 = DbAddress.JumpToFrame(2, jump1);
        jump2.SamePageJumpCount.Should().Be(2);
        jump2.GetFrameIndex().Should().Be(2);

        var jump3 = DbAddress.JumpToFrame(3, jump2);
        jump3.SamePageJumpCount.Should().Be(3);
        jump3.GetFrameIndex().Should().Be(3);
    }

    [Test]
    public void Equality()
    {
        var a1 = DbAddress.Page(1);
        var a2 = DbAddress.Page(2);

        // ReSharper disable once EqualExpressionComparison
        object.Equals(a1, a1).Should().BeTrue();
    }
}