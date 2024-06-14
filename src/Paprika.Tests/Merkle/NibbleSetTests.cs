using FluentAssertions;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NibbleSetTests
{
    private const int Max = 16;

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(15)]
    public void Single_nibble_set(byte nibble)
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
        @readonly.SmallestNibbleSet.Should().Be(nibble);
    }

    [TestCase(0, 1)]
    [TestCase(2, 15)]
    public void Double(byte nibble0, byte nibble1)
    {
        var set = new NibbleSet(nibble0, nibble1);
        NibbleSet.Readonly @readonly = set;

        for (byte i = 0; i < Max; i++)
        {
            var expected = i == nibble0 || i == nibble1;

            set[i].Should().Be(expected);
            @readonly[i].Should().Be(expected);

            set.SmallestNibbleSet.Should().Be(nibble0);
            @readonly.SmallestNibbleSet.Should().Be(nibble0);
        }
    }
}