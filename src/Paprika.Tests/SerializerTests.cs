using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Pages.Frames;

namespace Paprika.Tests;

public class SerializerTests
{
    [Test]
    public void EOA()
    {
        var path = NibblePath.FromKey(Keccak.Zero);
        var balance = UInt256.One;
        var nonce = UInt256.One;

        Span<byte> destination = stackalloc byte[Serializer.Account.EOAMaxByteCount];

        var leftover = Serializer.Account.WriteEOA(destination, path, balance, nonce);
        var actual = destination.Slice(0, destination.Length - leftover.Length);

        Serializer.Account.ReadAccount(actual, out var pathRead, out var balanceRead, out var nonceRead);

        pathRead.Equals(path).Should().BeTrue();
        balanceRead.Should().Be(balance);
        nonceRead.Should().Be(nonce);
    }
}