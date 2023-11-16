using System.Text;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.RLP;

namespace Paprika.Tests.Merkle;

public class HashingTests
{
    // NOTE: Expected results come from:
    // https://github.com/NethermindEth/nethermind/blob/bb7e792722085294b5adea54b598f3efd5887689/src/Nethermind/Nethermind.Trie.Test/PaprikaTrieTests.cs
    private static object[] _keysBalancesNoncesHexStrings =
    {
        new object[]
        {
            Values.Key0, Values.Balance0, Values.Nonce0,
            "E2533A0A0C4F1DDB72FEB7BFAAD12A83853447DEAAB6F28FA5C443DD2D37C3FB",
        },
        new object[]
        {
            Values.Key1A, Values.Balance1, Values.Nonce1,
            "DD358AF6B1D8E875FBA0E585710D054F14DD9D06FA3C8C5F2BAF66F413178F82",
        },
        new object[]
        {
            Values.Key2, Values.Balance2, Values.Nonce2,
            "A654F039A5F9E9F30C89F21555C92F1CB1E739AF11A9E9B12693DEDC6E76F628",
        },
        new object[]
        {
            Values.Key0, UInt256.MaxValue, Values.Nonce0,
            "FC790A3674B6847C609BB91B7014D67B2E70FF0E60F53AC49A8A45F1FECF35A6",
        },
        new object[]
        {
            Values.Key0, Values.Balance0, UInt256.MaxValue,
            "F5EC898875FFCF5F161718F09696ABEDA7B77DFB8F8CEBAFA57129F77C8D720B",
        },
    };

    [Test]
    [TestCaseSource(nameof(_keysBalancesNoncesHexStrings))]
    public void Account_leaf(Keccak key, UInt256 balance, UInt256 nonce, string hexString)
    {
        var account = new Account(balance, nonce);

        Node.Leaf.KeccakOrRlp(NibblePath.FromKey(key), account, out var computedHash);
        var expectedHash = new Keccak(Convert.FromHexString(hexString));

        Assert.That(computedHash.DataType, Is.EqualTo(KeccakOrRlp.Type.Keccak));
        Assert.That(new Keccak(computedHash.Span), Is.EqualTo(expectedHash));
    }

    [Test]
    public void Storage_leaf_small()
    {
        Span<byte> value = stackalloc byte[1] { 3 };
        Span<byte> key = stackalloc byte[1] { 1 };
        var path = NibblePath.FromKey(key);

        Node.Leaf.KeccakOrRlp(path, value, out var result);

        result.DataType.Should().Be(KeccakOrRlp.Type.Rlp);
        result.Span.ToArray().Should().BeEquivalentTo(new byte[] { 0xC4, 0x82, 0x20, 0x01, 0x03 });
    }

    // [Test]
    // public void Storage_leaf_big()
    // {
    //     const string key = "0209000d0e0c0d090504080b06020a080d06000304050a0908080308060f0c08040b0a060b0c09050408040000080f060306020f09030106000e0f030e050603";
    //     var path = NibblePath.FromKey(ParseNethermindKeyToHex(key));
    //     var value = Convert.FromHexString("945027d6cc532f94976ba255a3c8bc01ad7c0cd03c");
    //     
    //     Node.Leaf.KeccakOrRlp(path, value, out var result);
    //     
    //     result.DataType.Should().Be(KeccakOrRlp.Type.Keccak);
    //     
    //     var expectedHash = new Keccak(Convert.FromHexString("374b893364427e3d960d8e8d2e9048e789f19657245d4003a6b84212b27cd5ce"));
    //     new Keccak(result.Span).Should().Be(expectedHash);
    // }

    private static Keccak ParseNethermindKeyToHex(string key)
    {
        var sb = new StringBuilder();

        for (var i = 1; i < key.Length; i += 2)
        {
            sb.Append(key[i]);
        }

        return new Keccak(Convert.FromHexString(sb.ToString()));
    }
}
