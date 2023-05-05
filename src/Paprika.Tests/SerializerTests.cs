using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Pages;

namespace Paprika.Tests;

public class SerializerTests
{
    private static readonly UInt256 EthInWei = 1_000_000_000_000_000_000;
    private static readonly UInt256 Eth1000 = 1000UL * EthInWei;
    private static readonly UInt256 EthMax = 120_000_000UL * EthInWei;
    private static readonly UInt256 TotalTxCount = 2_000_000_000;

    [TestCaseSource(nameof(GetEOAData))]
    public void EOA(UInt256 balance, UInt256 nonce)
    {
        Span<byte> destination = stackalloc byte[Serializer.BalanceNonceMaxByteCount];

        var actual = Serializer.WriteAccount(destination, balance, nonce);

        Serializer.ReadAccount(actual, out var balanceRead, out var nonceRead);

        balanceRead.Should().Be(balance);
        nonceRead.Should().Be(nonce);
    }

    static IEnumerable<TestCaseData> GetEOAData()
    {
        yield return new TestCaseData(UInt256.Zero, UInt256.Zero).SetName("Zeros");
        yield return new TestCaseData(Eth1000, (UInt256)10000).SetName("Reasonable");
        yield return new TestCaseData(EthMax, TotalTxCount).SetName("Max");
    }
}