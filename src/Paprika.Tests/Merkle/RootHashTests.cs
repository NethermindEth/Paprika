using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class RootHashTests
{
    [Test]
    public void Empty_tree()
    {
        var commit = new Commit();

        AssertRoot("56E81F171BCC55A6FF8345E692C0F86E5B48E01B996CADC001622FB5E363B421", commit);
    }

    [Test]
    public void Single_account()
    {
        var commit = new Commit();

        var key = Values.Key0;
        var account = new Account(Values.Balance0, Values.Nonce0);

        commit.Set(Key.Account(key), account.WriteTo(stackalloc byte[Account.MaxByteCount]));

        AssertRoot("E2533A0A0C4F1DDB72FEB7BFAAD12A83853447DEAAB6F28FA5C443DD2D37C3FB", commit);
    }

    [Test]
    public void Branch_two_leafs()
    {
        var commit = new Commit();

        const byte nibbleA = 0x10;
        var balanceA = Values.Balance0;
        var nonceA = Values.Nonce0;

        const byte nibbleB = 0x20;
        var balanceB = Values.Balance1;
        var nonceB = Values.Nonce1;

        Span<byte> span = stackalloc byte[32];
        span.Fill(0);

        span[0] = nibbleA;
        commit.Set(Key.Account(new Keccak(span)), new Account(balanceA, nonceA).WriteTo(stackalloc byte[Account.MaxByteCount]));

        span[0] = nibbleB;
        commit.Set(Key.Account(new Keccak(span)), new Account(balanceB, nonceB).WriteTo(stackalloc byte[Account.MaxByteCount]));

        AssertRoot("73130daa1ae507554a72811c06e28d4fee671bfe2e1d0cef828a7fade54384f9", commit);
    }

    private static void AssertRoot(string hex, ICommit commit)
    {
        var merkle = new ComputeMerkleBehavior(true);

        merkle.BeforeCommit(commit);

        var keccak = new Keccak(Convert.FromHexString(hex));

        merkle.RootHash.Should().Be(keccak);
    }
}