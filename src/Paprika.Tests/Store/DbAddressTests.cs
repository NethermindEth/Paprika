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

    [TestCase(1U, 3)]
    [TestCase(256U, 3)]
    [TestCase(256U * 256U, 3)]
    [TestCase(256U * 256U * 256U, 4)]
    public void Write_read(uint value, int expectedLength)
    {
        var addr = DbAddress.Page(value);

        var written = addr.Write(stackalloc byte[DbAddress.Size]);

        written.Length.Should().Be(expectedLength);

        var read = DbAddress.Read(written);

        read.Raw.Should().Be(value);
    }
}
