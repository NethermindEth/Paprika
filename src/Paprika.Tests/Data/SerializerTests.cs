using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class SerializerTests
{
    [TestCaseSource(nameof(GetStorageValues))]
    public void Storage(UInt256 expected)
    {
        Span<byte> data = stackalloc byte[Serializer.MaxUint256SizeWithPrefix];
        var serialized = Serializer.WriteStorageValue(data, expected);
        Serializer.ReadStorageValue(serialized, out var actual);

        expected.Should().Be(actual);
    }

    public static IEnumerable<TestCaseData> GetStorageValues()
    {
        yield return new TestCaseData(UInt256.Zero);
        yield return new TestCaseData(UInt256.One);
        yield return new TestCaseData(new UInt256(ulong.MaxValue));
        yield return new TestCaseData(new UInt256(ulong.MaxValue, ulong.MaxValue));
        yield return new TestCaseData(new UInt256(ulong.MaxValue, ulong.MaxValue, ulong.MaxValue));
        yield return new TestCaseData(new UInt256(ulong.MaxValue, ulong.MaxValue, ulong.MaxValue, ulong.MaxValue));
    }
}