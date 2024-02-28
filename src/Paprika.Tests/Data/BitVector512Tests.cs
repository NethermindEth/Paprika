using FluentAssertions;
using NUnit.Framework;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class BitVector1024Tests
{
    [Test]
    public void Set_reset()
    {
        var v = new BitVector1024();

        for (int i = 0; i < BitVector1024.Count; i++)
        {
            v[i].Should().BeFalse();
            v[i] = true;
            v[i].Should().BeTrue();
            v[i] = false;
            v[i].Should().BeFalse();
        }
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(64)]
    [TestCase(65)]
    [TestCase(BitVector1024.Count - 1)]
    [TestCase(BitVector1024.Count)]
    public void First_not_set(int set)
    {
        var v = new BitVector1024();

        for (int i = 0; i < set; i++)
        {
            v[i] = true;
        }

        v.FirstNotSet.Should().Be((ushort)set);
    }

    [TestCase(1, true)]
    [TestCase(BitVector1024.Count, false)]
    public void Any_not_set(int set, bool anyNotSet)
    {
        var v = new BitVector1024();

        for (int i = 0; i < set; i++)
        {
            v[i] = true;
        }

        v.HasEmptyBits.Should().Be(anyNotSet);
    }
}