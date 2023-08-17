using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

/// <summary>
/// The tests are based on Nethermind's suite provided at
/// <see cref="https://github.com/NethermindEth/nethermind/blob/feature/paprika_merkle_tests/src/Nethermind/Nethermind.Trie.Test/PaprikaTrieTests.cs"/>
/// </summary>
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

    [TestCase(1000, "b255eb6261dc19f0639d13624e384b265759d2e4171c0eb9487e82d2897729f0")]
    [TestCase(10_000, "48864c880bd7610f9bad9aff765844db83c17cab764f5444b43c0076f6cf6c03")]
    public void Big_random(int count, string hexString)
    {
        var commit = new Commit();

        Random random = new(13);
        Span<byte> key = stackalloc byte[32];
        Span<byte> account = stackalloc byte[Account.MaxByteCount];

        for (int i = 0; i < count; i++)
        {
            random.NextBytes(key);
            uint value = (uint)random.Next();
            commit.Set(Key.Account(new Keccak(key)), new Account(value, value).WriteTo(account));
        }

        AssertRootFirst(hexString, commit);
        AssertRootSecond(hexString, commit);

        // use two separate method
        static void AssertRootFirst(string hex, Commit commit) => AssertRoot(hex, commit);
        static void AssertRootSecond(string hex, Commit commit) => AssertRoot(hex, commit);
    }

    [TestCase(1, "0c64fd8640de0ce2d0a928932ead5597214d5a3adc9c4c29056fef761791e374")]
    [TestCase(100, "6328bc0a47e3e9b6c3c0fd5b08693be48102f38a1d69dd0da0ef33679882d6ea")]
    [TestCase(1000, "ff60253c014966b6239710607f70363f52e6437777719c8320d567ab8a213055")]
    public void Big_random_storage(int count, string hexString)
    {
        var commit = new Commit();

        Random random = new(13);
        Span<byte> key = stackalloc byte[32];
        Span<byte> account = stackalloc byte[Account.MaxByteCount];

        for (int i = 0; i < count; i++)
        {
            random.NextBytes(key);
            
            var storageValue = random.Next();
            var value = (uint)random.Next();

            var a = new Account(value, value);
            var keccak = new Keccak(key);
            commit.Set(Key.Account(keccak), a.WriteTo(account));
            commit.Set(Key.StorageCell(NibblePath.FromKey(keccak), keccak), storageValue.ToByteArray());
        }

        AssertRoot(hexString, commit);
    }

    [Test]
    public void Extension()
    {
        var commit = new Commit();

        var balanceA = Values.Balance0;
        var nonceA = Values.Nonce0;

        var balanceB = Values.Balance1;
        var nonceB = Values.Nonce1;

        commit.Set(Key.Account(Values.Key0), new Account(balanceA, nonceA).WriteTo(stackalloc byte[Account.MaxByteCount]));
        commit.Set(Key.Account(Values.Key1), new Account(balanceB, nonceB).WriteTo(stackalloc byte[Account.MaxByteCount]));

        AssertRoot("a624947d9693a5cba0701897b3a48cb9954c2f4fd54de36151800eb2c7f6bf50", commit);
    }

    private static void AssertRoot(string hex, ICommit commit)
    {
        var merkle = new ComputeMerkleBehavior(true);

        merkle.BeforeCommit(commit);

        var keccak = new Keccak(Convert.FromHexString(hex));

        merkle.RootHash.Should().Be(keccak);
    }
}