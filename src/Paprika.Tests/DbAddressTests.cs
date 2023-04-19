﻿using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;
using Paprika.Pages.Frames;

namespace Paprika.Tests;

public class DbAddressTests
{
    [Test]
    public void Should_chain_in_page_properly()
    {
        DbAddress.Null.SamePageJumpCount.Should().Be(0);

        var jump1 = DbAddress.JumpToFrame(FrameIndex.FromIndex(1), DbAddress.Null);
        jump1.SamePageJumpCount.Should().Be(1);

        var jump2 = DbAddress.JumpToFrame(FrameIndex.FromIndex(2), jump1);
        jump2.SamePageJumpCount.Should().Be(2);
        jump2.TryGetFrameIndex(out var frame2).Should().BeTrue();
        frame2.Value.Should().Be(2);

        var jump3 = DbAddress.JumpToFrame(FrameIndex.FromIndex(3), jump2);
        jump3.SamePageJumpCount.Should().Be(3);
        jump3.TryGetFrameIndex(out var frame3).Should().BeTrue();
        frame3.Value.Should().Be(3);
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