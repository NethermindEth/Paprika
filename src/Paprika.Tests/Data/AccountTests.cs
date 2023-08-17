using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class AccountTests
{
    private static readonly UInt256 EthInWei = 1_000_000_000_000_000_000;
    private static readonly UInt256 Eth1000 = 1000UL * EthInWei;
    private static readonly UInt256 EthMax = 120_000_000UL * EthInWei;
    private static readonly UInt256 TotalTxCount = 2_000_000_000;

    [Test]
    public void EOA_Small_should_compress()
    {
        var expected = new Account(1, 1);
        var data = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        data.Length.Should().Be(3);

        Account.ReadFrom(data, out var account);
        account.Should().Be(expected);
    }

    [TestCaseSource(nameof(GetEoaData))]
    public void Eoa(UInt256 balance, UInt256 nonce)
    {
        var expected = new Account(balance, nonce);
        var data = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        Account.ReadFrom(data, out var account);
        account.Should().Be(expected);
    }

    [Test]
    public void Contract_Small_should_compress()
    {
        var expected = new Account(1, 1, CodeHash, StorageRoot);
        var data = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        data.Length.Should().Be(3 + Keccak.Size * 2);

        Account.ReadFrom(data, out var account);
        account.Should().Be(expected);
    }

    [TestCaseSource(nameof(GetContractData))]
    public void Contract(UInt256 balance, UInt256 nonce, Keccak codeHash, Keccak storageRootHash)
    {
        var expected = new Account(balance, nonce);
        var data = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        Account.ReadFrom(data, out var account);
        account.Should().Be(expected);
    }

    private static IEnumerable<TestCaseData> GetEoaData()
    {
        yield return new TestCaseData(UInt256.Zero, UInt256.Zero).SetName("Zeros");
        yield return new TestCaseData(Eth1000, (UInt256)10000).SetName("Reasonable");
        yield return new TestCaseData(EthMax, TotalTxCount).SetName("Max");
    }

    private static readonly Keccak CodeHash = Keccak.Compute(new byte[] { 0, 1, 2, 3 });
    private static readonly Keccak StorageRoot = Keccak.Compute(new byte[] { 0, 1, 2, 5, 6, 8 });

    private static IEnumerable<TestCaseData> GetContractData()
    {
        yield return new TestCaseData(UInt256.Zero, UInt256.Zero, CodeHash, StorageRoot).SetName("No balance, no nonce");
        yield return new TestCaseData(Eth1000, (UInt256)10000, CodeHash, StorageRoot).SetName("Reasonable");
        yield return new TestCaseData(EthMax, TotalTxCount, CodeHash, StorageRoot).SetName("Max");
    }
}