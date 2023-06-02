using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Tree;

namespace Paprika.Tests;

public class MerkleKeccakTests
{
    [Test]
    [TestCase(0, "E2533A0A0C4F1DDB72FEB7BFAAD12A83853447DEAAB6F28FA5C443DD2D37C3FB")]
    [TestCase(1, "DD358AF6B1D8E875FBA0E585710D054F14DD9D06FA3C8C5F2BAF66F413178F82")]
    [TestCase(2, "A654F039A5F9E9F30C89F21555C92F1CB1E739AF11A9E9B12693DEDC6E76F628")]
    public void Leaf_keccak(int index, string hexString)
    {
        var key = GetKey(index);
        var account = new Account(GetBalance(index), GetNonce(index));

        var computedHash = Leaf.ComputeKeccakOrRlp(NibblePath.FromKey(key), account);
        var expectedHash = new Keccak(Convert.FromHexString(hexString));

        Assert.That(computedHash.DataType, Is.EqualTo(KeccakOrRlp.Type.Keccak));
        Assert.That(new Keccak(computedHash.Data), Is.EqualTo(expectedHash));
    }

    private static Keccak GetKey(int keyNumber) =>
        keyNumber switch
        {
            0 => Values.Key0,
            1 => Values.Key1a,
            2 => Values.Key2,
            _ => throw new ArgumentOutOfRangeException(nameof(keyNumber))
        };

    private static UInt256 GetBalance(int balanceNumber) =>
        balanceNumber switch
        {
            0 => Values.Balance0,
            1 => Values.Balance1,
            2 => Values.Balance2,
            _ => throw new ArgumentOutOfRangeException(nameof(balanceNumber))
        };

    private static UInt256 GetNonce(int nonceNumber) =>
        nonceNumber switch
        {
            0 => Values.Nonce0,
            1 => Values.Nonce1,
            2 => Values.Nonce2,
            _ => throw new ArgumentOutOfRangeException(nameof(nonceNumber))
        };
}