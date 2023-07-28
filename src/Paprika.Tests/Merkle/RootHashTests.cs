using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class RootHashTests
{
    private Commit _commit = null!;
    private ComputeMerkleBehavior _merkle = null!;

    [SetUp]
    public void SetUp()
    {
        _commit = new Commit();
        _merkle = new ComputeMerkleBehavior(true);
    }

    [Test]
    public void Empty_tree()
    {
        AssertRoot("56E81F171BCC55A6FF8345E692C0F86E5B48E01B996CADC001622FB5E363B421");
    }

    private void AssertRoot(string hex)
    {
        _merkle.BeforeCommit(_commit);

        var keccak = new Keccak(Convert.FromHexString(hex));

        _merkle.RootHash.Should().Be(keccak);
    }

    [Test]
    public void Single_account()
    {
        var key = Values.Key0;
        var account = new Account(Values.Balance0, Values.Nonce0);

        _commit.Set(Key.Account(key), account.WriteTo(stackalloc byte[Account.MaxByteCount]));

        AssertRoot("E2533A0A0C4F1DDB72FEB7BFAAD12A83853447DEAAB6F28FA5C443DD2D37C3FB");
    }
}