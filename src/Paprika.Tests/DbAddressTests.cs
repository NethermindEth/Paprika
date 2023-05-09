﻿using FluentAssertions;
using NUnit.Framework;
using Paprika.Db;
using Paprika.Pages;

namespace Paprika.Tests;

public class DbAddressTests
{
    [Test]
    public void Equality()
    {
        var a1 = DbAddress.Page(1);
        var a2 = DbAddress.Page(2);

        // ReSharper disable once EqualExpressionComparison
        object.Equals(a1, a1).Should().BeTrue();
    }
}