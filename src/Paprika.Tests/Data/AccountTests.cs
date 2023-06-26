using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class AccountTests
{
    private static readonly UInt256 EthInWei = 1_000_000_000_000_000_000;
    private static readonly UInt256 Eth1000 = 1000UL * EthInWei;
    private static readonly UInt256 EthMax = 120_000_000UL * EthInWei;
    private static readonly UInt256 TotalTxCount = 2_000_000_000;

    [Test]
    public void Small_should_compress()
    {
        var expected = new Account(1, 1);
        var data = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        data.Length.Should().Be(3);

        Account.ReadFrom(data, out var account);
        account.Should().Be(expected);
    }

    [TestCaseSource(nameof(GetEOAData))]
    public void EOA(UInt256 balance, UInt256 nonce)
    {
        var expected = new Account(balance, nonce);
        var data = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        Account.ReadFrom(data, out var account);
        account.Should().Be(expected);
    }

    private static IEnumerable<TestCaseData> GetEOAData()
    {
        yield return new TestCaseData(UInt256.Zero, UInt256.Zero).SetName("Zeros");
        yield return new TestCaseData(Eth1000, (UInt256)10000).SetName("Reasonable");
        yield return new TestCaseData(EthMax, TotalTxCount).SetName("Max");
    }
}