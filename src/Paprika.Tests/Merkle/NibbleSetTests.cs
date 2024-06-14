using FluentAssertions;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NibbleSetTests
{
    private const int Max = 16;
    private static readonly IEnumerable<byte> Indexes = Enumerable.Range(0, Max).Select(i => (byte)i);

    [TestCaseSource(nameof(GetOneNibble))]
    public void One_nibble(byte nibble)
    {
        var set = new NibbleSet(nibble);
        NibbleSet.Readonly @readonly = set;

        for (byte i = 0; i < Max; i++)
        {
            var expected = i == nibble;

            set[i].Should().Be(expected);
            @readonly[i].Should().Be(expected);
        }

        set.SmallestNibbleSet.Should().Be(nibble);
        set.BiggestNibbleSet.Should().Be(nibble);

        @readonly.SmallestNibbleSet.Should().Be(nibble);
        @readonly.BiggestNibbleSet.Should().Be(nibble);
    }

    [TestCaseSource(nameof(GetTwoNibbles))]
    public void Two_nibbles(byte nibble0, byte nibble1)
    {
        var min = Math.Min(nibble0, nibble1);
        var max = Math.Max(nibble0, nibble1);

        var set = new NibbleSet(nibble0, nibble1);
        NibbleSet.Readonly @readonly = set;

        for (byte i = 0; i < Max; i++)
        {
            var expected = i == nibble0 || i == nibble1;

            set[i].Should().Be(expected);
            @readonly[i].Should().Be(expected);
        }

        set.SmallestNibbleSet.Should().Be(min);
        set.BiggestNibbleSet.Should().Be(max);
        @readonly.SmallestNibbleSet.Should().Be(min);
        @readonly.BiggestNibbleSet.Should().Be(max);
    }

    [TestCaseSource(nameof(GetTwoNibbles))]
    public void All_but_two(byte nibble0, byte nibble1)
    {
        var @readonly = NibbleSet.Readonly.All
            .Remove(nibble0)
            .Remove(nibble1);

        for (byte i = 0; i < Max; i++)
        {
            var expected = i != nibble0 && i != nibble1;
            @readonly[i].Should().Be(expected);
        }

        var min = Indexes.First(i => @readonly[i]);
        var max = Indexes.Last(i => @readonly[i]);

        @readonly.SmallestNibbleSet.Should().Be(min);
        @readonly.BiggestNibbleSet.Should().Be(max);
    }

    [TestCaseSource(nameof(GetOneNibble))]
    public void All_but_one(byte nibble)
    {
        var @readonly = NibbleSet.Readonly.AllWithout(nibble);

        for (byte i = 0; i < Max; i++)
        {
            var expected = i != nibble;
            @readonly[i].Should().Be(expected);
        }

        @readonly.SmallestNibbleNotSet.Should().Be(nibble);
    }

    private static TestCaseData[] GetTwoNibbles() =>
    [
        new TestCaseData((byte)0, (byte)1),
        new TestCaseData((byte)2, (byte)15),
        new TestCaseData((byte)13, (byte)2)
    ];

    private static TestCaseData[] GetOneNibble() =>
    [
        new TestCaseData((byte)0),
        new TestCaseData((byte)1),
        new TestCaseData((byte)15)
    ];
}